package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{ExecutionState, ExecutionTrace, OperationTrace}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class ExecutionTraceRecord(id: Option[Long],
                                             operationTraceId: Long,
                                             logHref: Option[String] = None,
                                             state: ExecutionState = ExecutionState.pending) {
  def toExecutionTrace(operationTrace: OperationTrace): ExecutionTrace = {
    ExecutionTrace(id.get, operationTrace, logHref, state)
  }
}


trait ExecutionTraceBinder extends TableBinder {
  this: OperationTraceBinder with DbContextProvider =>

  import dbContext.driver.api._

  private implicit lazy val stateMapper = MappedColumnType.base[ExecutionState, Short](
    es => es.id.toShort,
    short => ExecutionState(short.toInt)
  )

  class ExecutionTraceTable(tag: Tag) extends Table[ExecutionTraceRecord](tag, "execution_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def operationTraceId = column[Long]("operation_trace_id")
    protected def fk = foreignKey(operationTraceId, operationTraceQuery)(_.id)

    def logHref = column[Option[String]]("log_href", O.SqlType("nvarchar(128)"))
    protected def logHrefIdx = index(logHref, unique = true)

    def state = column[ExecutionState]("state")
    protected def stateIdx = index(state)

    def * = (id.?, operationTraceId, logHref, state) <> (ExecutionTraceRecord.tupled, ExecutionTraceRecord.unapply)
  }

  val executionTraceQuery = TableQuery[ExecutionTraceTable]

  def addToOperationTrace(traceId: Long, numberOfTraces: Int): Future[Seq[Long]] = {
    val execTrace = ExecutionTraceRecord(None, traceId)
    dbContext.db.run((executionTraceQuery returning executionTraceQuery.map(_.id)) ++= List.fill(numberOfTraces)(execTrace))
  }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Seq[ExecutionTrace]] = {
    val query = executionTraceQuery join operationTraceQuery on (_.operationTraceId === _.id) filter (_._2.deploymentRequestId === deploymentRequestId)
    dbContext.db.run(query.result).map(_.map {
      case (exec, op) => exec.toExecutionTrace(op.toOperationTrace)
    })
  }

  def findExecutionTraceById(executionTraceId: Long): Future[Option[ExecutionTrace]] = {
    val query = executionTraceQuery join operationTraceQuery on (_.operationTraceId === _.id) filter (_._1.id === executionTraceId)
    dbContext.db.run(query.result).map(_.headOption.map {
      case (exec, op) => exec.toExecutionTrace(op.toOperationTrace)
    })
  }

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
