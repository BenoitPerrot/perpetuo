package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.UnavailableAction
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.google.common.annotations.VisibleForTesting
import slick.sql.FixedSqlAction

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class ExecutionTraceRecord(id: Option[Long],
                                             executionId: Long,
                                             executorType: String,
                                             href: Option[String] = None,
                                             state: ExecutionState = ExecutionState.pending,
                                             detail: Comment = "") {
  def toExecutionTrace: ShallowExecutionTrace =
    ShallowExecutionTrace(id.get, href, state, detail.toString)

  def toExecutionTrace(operationTrace: OperationTrace): ExecutionTraceBranch =
    ExecutionTraceBranch(id.get, executionId, operationTrace, href, state, detail.toString)
}


trait ExecutionTraceBinder extends TableBinder {
  this: ExecutionBinder with OperationTraceBinder with DbContextProvider =>

  import dbContext.profile.api._

  protected implicit lazy val stateMapper = MappedColumnType.base[ExecutionState, Short](
    es => es.id.toShort,
    short => ExecutionState(short.toInt)
  )

  class ExecutionTraceTable(tag: Tag) extends Table[ExecutionTraceRecord](tag, "execution_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def executionId = column[Long]("execution_id")
    protected def fk = foreignKey(executionId, executionQuery)(_.id)

    def executorType = column[String]("executor_type", O.SqlType("nvarchar(64)"))
    def href = column[Option[String]]("href", O.SqlType("nvarchar(1024)"))
    def state = column[ExecutionState]("state")
    def detail = nvarchar[Comment]("detail")

    def * = (id.?, executionId, executorType, href, state, detail) <> (ExecutionTraceRecord.tupled, ExecutionTraceRecord.unapply)
  }

  val executionTraceQuery = TableQuery[ExecutionTraceTable]

  def insertingExecutionTraces(traces: Iterable[ExecutionTraceRecord]): FixedSqlAction[Seq[Long], NoStream, Effect.Write] =
    (executionTraceQuery returning executionTraceQuery.map(_.id)) ++= traces

  private def openExecutionTracesQuery(operationTraceId: Long) =
    executionQuery
      .join(executionTraceQuery)
      .filter { case (execution, executionTrace) =>
        execution.operationTraceId === operationTraceId && execution.id === executionTrace.executionId &&
          (executionTrace.state === ExecutionState.pending || executionTrace.state === ExecutionState.running)
      }

  def hasOpenExecutionTracesForOperation(operationTraceId: Long): FixedSqlAction[Boolean, NoStream, Effect.Read] =
    openExecutionTracesQuery(operationTraceId).exists.result

  def findingOpenExecutionTracesByOperationTrace(operationTraceId: Long): DBIOAction[Seq[ShallowExecutionTrace], NoStream, Effect.Read] =
    openExecutionTracesQuery(operationTraceId)
      .map { case (_, executionTrace) => executionTrace }
      .result
      .map(_.map(_.toExecutionTrace))

  /**
    * @return Some(id) if it has been successfully updated, None if it doesn't exist
    * @throws UnavailableAction if the transition is not allowed
    */
  def updatingExecutionTrace(id: Long, state: ExecutionState, detail: String, href: Option[String] = None): DBIOrw[Option[Long]] = {
    href
      .map(_ => runningUpdate(id, state, _.map(r => (r.state, r.detail, r.href)).update((state, detail, href))))
      .getOrElse(runningUpdate(id, state, _.map(r => (r.state, r.detail)).update((state, detail))))
  }

  private def runningUpdate(id: Long, state: ExecutionState, updateQuery: Query[ExecutionTraceTable, ExecutionTraceRecord, Seq] => DBIOAction[Int, NoStream, Effect.Write]) = {
    val filterQuery = executionTraceQuery.filter(executionTrace =>
      executionTrace.id === id && executionTrace.state.inSet(state :: ExecutionState.getPredecessors(state))
    )
    updateQuery(filterQuery).flatMap { updatedCount =>
      // only in the case no record matched the filter (which is an error from the user, so exceptional...), we check whether the execution trace exists
      if (updatedCount == 0)
        executionTraceQuery.filter(_.id === id).result.map(_.headOption.map(executionTrace =>
          throw UnavailableAction(s"Cannot transition an execution from `${executionTrace.state}` to `$state`", Map("executionTraceId" -> id))
        ))
      else
        DBIO.successful(Some(id))
    }
  }

  def findExecutionTraceIdsByExecution(executionId: Long): Future[Seq[Long]] =
    dbContext.db.run(
      executionTraceQuery
        .filter(_.executionId === executionId)
        .map(_.id)
        .result
    )

  @VisibleForTesting
  def findExecutionTracesByOperationTrace(operationTraceId: Long): Future[Seq[ShallowExecutionTrace]] =
    dbContext.db.run(
      executionTraceQuery.join(executionQuery)
        .filter { case (trace, execution) =>
          trace.executionId === execution.id && execution.operationTraceId === operationTraceId
        }
        .map { case (trace, _) => trace }
        .result
        .map(_.map(_.toExecutionTrace))
    )
}
