package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.app.DbContext
import com.criteo.perpetuo.dao.enums.ExecutionState.ExecutionState
import com.criteo.perpetuo.dao.enums.{ExecutionState => ExecutionStateType}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class ExecutionTrace(id: Option[Long],
                          operationTraceId: Long,
                          uuid: Option[String] = None,
                          state: ExecutionState = ExecutionStateType.pending)


trait ExecutionTraceBinder extends TableBinder {
  this: OperationTraceBinder with DbContextProvider =>

  import dbContext.driver.api._

  private implicit lazy val stateMapper = MappedColumnType.base[ExecutionState, Short](
    es => es.id.toShort,
    short => ExecutionStateType(short.toInt)
  )

  class ExecutionTraceTable(tag: Tag) extends Table[ExecutionTrace](tag, "execution_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def operationTraceId = column[Long]("operation_trace_id")
    protected def fk = foreignKey(operationTraceId, operationTraceQuery)(_.id)

    def uuid = column[Option[String]]("uuid", O.SqlType("nchar(128)"))
    protected def uuidIdx = index(uuid, unique = true)

    def state = column[ExecutionState]("state")
    protected def stateIdx = index(state)

    def * = (id.?, operationTraceId, uuid, state) <> (ExecutionTrace.tupled, ExecutionTrace.unapply)
  }

  val executionTraceQuery = TableQuery[ExecutionTraceTable]

  def addToOperationTrace(traceId: Long, numberOfTraces: Int): Future[Seq[Long]] = {
    val execTrace = ExecutionTrace(None, traceId)
    dbContext.db.run((executionTraceQuery returning executionTraceQuery.map(_.id)) ++= List.fill(numberOfTraces)(execTrace))
  }

  def findExecutionTraceById(id: Long): Future[Option[ExecutionTrace]] = {
    dbContext.db.run(executionTraceQuery.filter(_.id === id).result).map(_.headOption)
  }

  def findExecutionTracesByOperationTrace(operationTraceId: Long): Future[Seq[ExecutionTrace]] = {
    dbContext.db.run(executionTraceQuery.filter(_.operationTraceId === operationTraceId).result)
  }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Seq[ExecutionTrace]] = {
    val query = for {
      (exec, op) <- executionTraceQuery join operationTraceQuery on (_.operationTraceId === _.id) if op.deploymentRequestId === deploymentRequestId
    } yield exec
    dbContext.db.run(query.result)
  }

  case class updateExecutionTrace(id: Long) {
    def apply(uuid: String, state: ExecutionState): Future[Unit] =
      run(_.map(r => (r.uuid, r.state)).update((Some(uuid), state)))
    def apply(uuid: String): Future[Unit] =
      run(_.map(_.uuid).update(Some(uuid)))
    def apply(state: ExecutionState): Future[Unit] =
      run(_.map(_.state).update(state))

    private def run(query: Query[ExecutionTraceTable, ExecutionTrace, Seq] => DBIOAction[Int, NoStream, Effect.Write]): Future[Unit] =
      dbContext.db.run(query(executionTraceQuery.filter(_.id === id))).map(count => assert(count == 1))
  }

}


@Singleton
class ExecutionTraceBinding @Inject()(val dbContext: DbContext) extends ExecutionTraceBinder
  with OperationTraceBinder with DeploymentRequestBinder with DbContextProvider
