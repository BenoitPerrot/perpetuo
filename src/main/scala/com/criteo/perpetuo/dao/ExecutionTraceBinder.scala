package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.ExecutionState._
import com.criteo.perpetuo.model.{ExecutionState, ExecutionTrace, ShallowOperationTrace}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class ExecutionTraceRecord(id: Option[Long],
                                             executionId: Long,
                                             logHref: Option[String] = None,
                                             state: ExecutionState = ExecutionState.pending) {
  def toExecutionTrace(operationTrace: ShallowOperationTrace): ExecutionTrace = {
    ExecutionTrace(id.get, executionId, operationTrace, logHref, state)
  }
}


trait ExecutionTraceBinder extends TableBinder {
  this: ExecutionBinder with OperationTraceBinder with DbContextProvider =>

  import dbContext.driver.api._

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

    def * = (id.?, executionId, logHref, state) <> (ExecutionTraceRecord.tupled, ExecutionTraceRecord.unapply)
  }

  val executionTraceQuery = TableQuery[ExecutionTraceTable]

  def insertExecutionTraces(executionId: Long, numberOfTraces: Int): Future[Seq[Long]] = { // fixme: deprecated
    val execTrace = ExecutionTraceRecord(None, executionId)
    dbContext.db.run((executionTraceQuery returning executionTraceQuery.map(_.id)) ++= List.fill(numberOfTraces)(execTrace)).map { seq =>
      assert(seq.length == numberOfTraces)
      seq
    }
  }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Seq[ExecutionTrace]] =
    dbContext.db.run(
      operationTraceQuery
        .filter(_.deploymentRequestId === deploymentRequestId)
        .join(executionQuery).on { case (operationTrace, execution) => operationTrace.id === execution.operationTraceId }
        .join(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.id === executionTrace.executionId }
        .map { case ((operationTrace, _), executionTrace) => (executionTrace, operationTrace) }
        .result
    ).map(_.map { case (exec, op) =>
      exec.toExecutionTrace(op.toOperationTrace)
    })

  def findExecutionTraceById(executionTraceId: Long): Future[Option[ExecutionTrace]] =
    dbContext.db.run(
      executionTraceQuery
        .filter(_.id === executionTraceId)
        .join(executionQuery).on { case (executionTrace, execution) => executionTrace.executionId === execution.id }
        .join(operationTraceQuery).on { case ((_, execution), operationTrace) => execution.operationTraceId === operationTrace.id }
        .map { case ((executionTrace, _), operationTrace) => (executionTrace, operationTrace) }
        .result
    ).map(_.headOption.map { case (exec, op) =>
      exec.toExecutionTrace(op.toOperationTrace)
    })

  def hasOpenExecutionTracesForOperation(operationTraceId: Long): Future[Boolean] =
    dbContext.db.run(
      operationTraceQuery
        .filter(_.id === operationTraceId)
        .join(executionQuery).on { case (operationTrace, execution) => operationTrace.id === execution.operationTraceId }
        .join(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.id === executionTrace.executionId }
        .filter { case ((_, _), executionTrace) => executionTrace.state === ExecutionState.pending || executionTrace.state === ExecutionState.running }
        .exists
        .result
    )

  def updateExecutionTrace(id: Long, logHref: String, state: ExecutionState): Future[Boolean] =
    runUpdate(id, _.map(r => (r.logHref, r.state)).update((Some(logHref), state)))

  def updateExecutionTrace(id: Long, state: ExecutionState): Future[Boolean] =
    runUpdate(id, _.map(_.state).update(state))

  private def runUpdate(id: Long, query: Query[ExecutionTraceTable, ExecutionTraceRecord, Seq] => DBIOAction[Int, NoStream, Effect.Write]): Future[Boolean] =
    dbContext.db.run(query(executionTraceQuery.filter(_.id === id))).map(count => {
      assert(count <= 1)
      count == 1
    })
}
