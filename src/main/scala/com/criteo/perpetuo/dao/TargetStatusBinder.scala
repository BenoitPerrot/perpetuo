package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{Status, TargetAtom, TargetAtomStatus, TargetStatus}
import slick.jdbc.TransactionIsolation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class TargetStatusRecord(executionId: Long,
                                           targetAtom: String,
                                           code: Status.Code,
                                           detail: String) {
  def toTargetStatus: TargetStatus =
    TargetStatus(targetAtom, code, detail)
}


trait TargetStatusBinder extends TableBinder {
  this: ExecutionBinder with DbContextProvider =>

  import dbContext.driver.api._

  protected implicit lazy val statusMapper = MappedColumnType.base[Status.Code, Short](
    op => op.id.toShort,
    short => Status(short.toInt)
  )

  class TargetStatusTable(tag: Tag) extends Table[TargetStatusRecord](tag, "target_status") {
    def executionId = column[Long]("execution_id")
    protected def fk = foreignKey(executionId, executionQuery)(_.id)

    def targetAtom = column[TargetAtom.Type]("target", O.SqlType(s"nvarchar(${TargetAtom.maxSize})"))
    def code = column[Status.Code]("code")
    def detail = column[String]("detail", O.SqlType(s"nvarchar(4000)"))

    protected def pk = primaryKey((targetAtom, executionId))

    def * = (executionId, targetAtom, code, detail) <> (TargetStatusRecord.tupled, TargetStatusRecord.unapply)
  }

  val targetStatusQuery: TableQuery[TargetStatusTable] = TableQuery[TargetStatusTable]

  def updateTargetStatuses(executionId: Long, statusMap: Map[String, TargetAtomStatus]): Future[Unit] = {
    if (statusMap.isEmpty)
      return Future.successful()

    val atoms = statusMap.keySet
    val oldValues = targetStatusQuery.filter(ts => ts.executionId === executionId && ts.targetAtom.inSet(atoms))
    val newValues = statusMap.map { case (atom, status) =>
      TargetStatusRecord(executionId, atom, status.code, status.detail)
    }

    // there is no bulk insert-or-update, so to remain efficient in case of many statuses,
    // we delete them all and re-insert them in the same transaction
    val query = oldValues.delete.andThen(targetStatusQuery ++= newValues)
      .transactionally.withTransactionIsolation(TransactionIsolation.Serializable)

    dbContext.db.run(query).map(_ => ())
  }
}
