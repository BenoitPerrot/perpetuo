package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.UnavailableAction
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.google.common.annotations.VisibleForTesting
import slick.sql.{FixedSqlAction, FixedSqlStreamingAction}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class ExecutionTraceRecord(id: Option[Long],
                                             executionId: Long,
                                             logHref: Option[String] = None,
                                             state: ExecutionState = ExecutionState.pending,
                                             detail: String = "") {
  def toExecutionTrace: ShallowExecutionTrace =
    ShallowExecutionTrace(id.get, logHref, state, detail)

  def toExecutionTrace(operationTrace: OperationTrace): ExecutionTraceBranch =
    ExecutionTraceBranch(id.get, executionId, operationTrace, logHref, state, detail)
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

    def logHref = column[Option[String]]("log_href", O.SqlType("nvarchar(1024)"))
    def state = column[ExecutionState]("state")
    def detail = column[String]("detail", O.SqlType("nvarchar(1024)"))

    def * = (id.?, executionId, logHref, state, detail) <> (ExecutionTraceRecord.tupled, ExecutionTraceRecord.unapply)
  }

  val executionTraceQuery = TableQuery[ExecutionTraceTable]

  def insertingExecutionTraces(executionId: Long, numberOfTraces: Int): FixedSqlAction[Seq[Long], NoStream, Effect.Write] =
    insertingExecutionTraces(executionId, List.fill(numberOfTraces)(ExecutionTraceRecord(None, executionId)))

  def insertingExecutionTraces(executionId: Long, traces: Iterable[ExecutionTraceRecord]): FixedSqlAction[Seq[Long], NoStream, Effect.Write] =
    (executionTraceQuery returning executionTraceQuery.map(_.id)) ++= traces

  // todo: this method should actually not exist, it's a dangerous shortcut: migrate clients and remove
  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Seq[ShallowExecutionTrace]] =
    dbContext.db.run(
      operationTraceQuery
        .filter(_.deploymentRequestId === deploymentRequestId)
        .join(executionQuery).on { case (operationTrace, execution) => operationTrace.id === execution.operationTraceId }
        .join(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.id === executionTrace.executionId }
        .map { case (_, executionTrace) => executionTrace }
        .result
    ).map(_.map(_.toExecutionTrace))

  def findingOperationTracesByDeploymentRequest(deploymentRequest: DeploymentRequest): DBIOAction[Seq[OperationTrace], NoStream, Effect.Read] =
    operationTraceQuery.filter(_.deploymentRequestId === deploymentRequest.id).result.map(_.map(_.toOperationTrace(deploymentRequest)))

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
  def updatingExecutionTrace(id: Long, state: ExecutionState, detail: String, logHref: Option[String] = None): DBIOrw[Option[Long]] =
    logHref
      .map(_ => runningUpdate(id, state, _.map(r => (r.state, r.detail, r.logHref)).update((state, detail, logHref))))
      .getOrElse(runningUpdate(id, state, _.map(r => (r.state, r.detail)).update((state, detail))))

  private def runningUpdate(id: Long, state: ExecutionState, updateQuery: Query[ExecutionTraceTable, ExecutionTraceRecord, Seq] => DBIOAction[Int, NoStream, Effect.Write]) = {
    val filterQuery = executionTraceQuery.filter(executionTrace =>
      executionTrace.id === id && executionTrace.state.inSet(state :: ExecutionState.getPredecessors(state))
    )
    updateQuery(filterQuery).flatMap { updatedCount =>
      // only in the case no record matched the filter (which is an error from the user, so exceptional...), we check whether the execution trace exists
      if (updatedCount == 0)
        executionTraceQuery.filter(_.id === id).result.map(_.headOption.map(executionTrace =>
          throw UnavailableAction(s"Cannot transition an execution from `${executionTrace.state}` to `$state`")
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
