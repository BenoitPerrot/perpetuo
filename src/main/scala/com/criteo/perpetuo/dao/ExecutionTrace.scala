package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao.enums.ExecutionState.ExecutionState
import com.criteo.perpetuo.dao.enums.{ExecutionState => ExecutionStateType}
import slick.driver.JdbcProfile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class ExecutionTrace(id: Option[Long],
                          operationTraceId: Long,
                          uuid: Option[String] = None,
                          state: ExecutionState = ExecutionStateType.pending)


trait ExecutionTraceBinder extends TableBinder {
  this: OperationTraceBinder with ProfileProvider =>

  import profile.api._

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

  def addToOperationTrace(db: Database, traceId: Long, numberOfTraces: Int): Future[Seq[Long]] = {
    val execTrace = ExecutionTrace(None, traceId)
    db.run((executionTraceQuery returning executionTraceQuery.map(_.id)) ++= List.fill(numberOfTraces)(execTrace))
  }

  def findExecutionTraceById(db: Database, id: Long): Future[Option[ExecutionTrace]] = {
    db.run(executionTraceQuery.filter(_.id === id).result).map(_.headOption)
  }
}


@Singleton
class ExecutionTraceBinding @Inject()(val profile: JdbcProfile) extends ExecutionTraceBinder
  with OperationTraceBinder with DeploymentRequestBinder with ProfileProvider
