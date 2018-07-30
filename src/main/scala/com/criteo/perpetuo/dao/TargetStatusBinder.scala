package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{Status, TargetAtom, TargetAtomStatus, TargetStatus}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}


private[dao] case class TargetStatusRecord(executionId: Long,
                                           targetAtom: String,
                                           code: Status.Code,
                                           detail: Comment) {
  def toTargetStatus: TargetStatus =
    TargetStatus(targetAtom, code, detail.toString)
}


trait TargetStatusBinder extends TableBinder {
  this: ExecutionBinder with DbContextProvider =>

  import dbContext.profile.api._

  protected implicit lazy val statusMapper = MappedColumnType.base[Status.Code, Short](
    op => op.id.toShort,
    short => Status(short.toInt)
  )

  class TargetStatusTable(tag: Tag) extends Table[TargetStatusRecord](tag, "target_status") {
    def executionId = column[Long]("execution_id")
    protected def fk = foreignKey(executionId, executionQuery)(_.id)

    def targetAtom = column[TargetAtom.Type]("target", O.SqlType(s"nvarchar(${TargetAtom.maxSize})"))
    def code = column[Status.Code]("code")
    def detail = nvarchar[Comment]("detail")

    protected def pk = primaryKey((targetAtom, executionId))

    def * = (executionId, targetAtom, code, detail) <> (TargetStatusRecord.tupled, TargetStatusRecord.unapply)
  }

  val targetStatusQuery: TableQuery[TargetStatusTable] = TableQuery[TargetStatusTable]

  def findTargetsByExecution(executionId: Long): Future[Seq[String]] =
    dbContext.db.run(targetStatusQuery.filter(_.executionId === executionId).map(_.targetAtom).result)

  /**
    * Hard-core implementation of a SQL bulk and transaction-free insert-or-update.
    * - The base idea is that the state machine describing the target statuses and their transitions is acyclic
    * (it's obvious for the status codes, it's also true for the statuses themselves, which are inserted but never removed).
    * - The other idea is that an unlimited sequence of retries of the same call must eventually converge if it's
    * not a bad request: if it's timing out because of too many records to create or update, the target statuses are
    * considered independent so this function must apply as many changes as possible in order for the next client's
    * attempts to have a chance to succeed.
    */
  def updatingTargetStatuses(executionId: Long, statusMap: Map[String, TargetAtomStatus]): DBIOrw[Unit] = {
    targetStatusQuery
      // first look at what is already created (nothing is ever removed) in order to
      // decrease the size of the following chain of inserts and updates (which can be costly)
      .filter(ts => ts.executionId === executionId && ts.targetAtom.inSet(statusMap.keySet))
      .result
      .map { currentRecords =>
        // todo: take the status code "precedence" into account in order to reject impossible transitions, once DREDD-725 is implemented
        val (same, different) = currentRecords.partition { currentRecord =>
          val askedStatus = statusMap(currentRecord.targetAtom)
          // look at the records that are up-to-date as of now, so we won't touch them at all
          currentRecord.code == askedStatus.code && currentRecord.detail.toString == askedStatus.detail
        }
        (same.map(_.targetAtom), different.map(_.targetAtom))
      }
      .flatMap { case (same, different) =>
        val alreadyCreated = (same.toStream ++ different).toSet
        val toUpdate = Iterable.newBuilder[String] ++= different
        DBIO.sequence(
          statusMap.toStream.collect { case (atom, status) if !alreadyCreated(atom) =>
            // try to create missing atoms
            (targetStatusQuery += TargetStatusRecord(executionId, atom, status.code, status.detail)).asTry.map {
              case Success(_) => ()
              case Failure(_) => toUpdate += atom // it has possibly been created meanwhile but we don't know with which values
            }
          }
        ).andThen(DBIO.sequence(
          toUpdate
            .result
            // for efficiency purpose, try to chain as less requests as possible by gathering the atoms sharing the same status
            .groupBy(statusMap)
            .map { case (TargetAtomStatus(newCode, newDetail), atomsToUpdate) =>
              targetStatusQuery
                .filter(ts => ts.executionId === executionId && ts.targetAtom.inSet(atomsToUpdate)) // we could reject impossible transitions here too but it's not that important: see below
                .map(ts => (ts.code, ts.detail))
                // there might be a race condition here, but we don't care: if two requests give different results
                // at the "same time" for the same target, we don't know which one is right anyway
                .update(newCode, newDetail)
            }
        ))
      }
      .map(_ => ())
      .withPinnedSession
  }

  def closingTargetStatuses(operationTraceId: Long): DBIOrw[Int] =
    executionQuery.filter(_.operationTraceId === operationTraceId).map(_.id).result
      .flatMap(executionIds =>
        targetStatusQuery
          .filter(targetStatus => targetStatus.code === Status.running && targetStatus.executionId.inSet(executionIds))
          .map(targetStatus => (targetStatus.code, targetStatus.detail))
          .update((Status.undetermined, "No feedback from the executor"))
      )
}
