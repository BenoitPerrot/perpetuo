package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao.enums.ExecutionState.ExecutionState
import com.criteo.perpetuo.dao.enums.{ExecutionState => ExecutionStateType}
import slick.driver.JdbcProfile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class ExecutionTrace(id: Option[Long],
                          operationTraceId: Long,
                          guid: String, // Optional, but it's easier to consider that no guid <=> empty guid
                          state: ExecutionState)


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

    def guid = column[String]("guid", O.SqlType("nvarchar(128)")) // should this be made unique?
    def state = column[ExecutionState]("state")
    protected def idx = index(state)

    def * = (id.?, operationTraceId, guid, state) <> (ExecutionTrace.tupled, ExecutionTrace.unapply)
  }

  val executionTraceQuery = TableQuery[ExecutionTraceTable]

  def addTo(db: Database, operationTrace: OperationTrace): Future[Long] = {
    addToOperationTrace(db, operationTrace.id.get)
  }

  def addToOperationTrace(db: Database, operationTraceId: Long): Future[Long] = {
    val execTrace = ExecutionTrace(None, operationTraceId, "", ExecutionStateType.pending)
    db.run((executionTraceQuery returning executionTraceQuery.map(_.id)) += execTrace)
  }

  def findExecutionTraceById(db: Database, id: Long): Future[Option[ExecutionTrace]] = {
    db.run(executionTraceQuery.filter(_.id === id).result).map(_.headOption)
  }
}


@Singleton
class ExecutionTraceBinding @Inject()(val profile: JdbcProfile) extends ExecutionTraceBinder
  with OperationTraceBinder with DeploymentRequestBinder with ProfileProvider
