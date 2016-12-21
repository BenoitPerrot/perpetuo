package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao.enums.ExecutionState
import slick.driver.JdbcProfile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class ExecutionTrace(id: Option[Long],
                          deploymentTraceId: Long,
                          guid: String, // Optional, but it's easier to consider that no guid <=> empty guid
                          state: ExecutionState.Type)


trait ExecutionTraceBinder extends TableBinder {
  this: DeploymentTraceBinder with ProfileProvider =>

  import profile.api._

  implicit lazy val stateMapper = MappedColumnType.base[ExecutionState.Type, Short](
    es => es.id.toShort,
    short => ExecutionState(short.toInt)
  )

  class ExecutionTraceTable(tag: Tag) extends Table[ExecutionTrace](tag, "execution_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def deploymentTraceId = column[Long]("deployment_trace_id")
    protected def fk = foreignKey(deploymentTraceId, deploymentTraceQuery)(_.id)

    def guid = column[String]("guid", O.SqlType("nvarchar(128)")) // should this be made unique?
    def state = column[ExecutionState.Type]("state")
    protected def idx = index(state)

    def * = (id.?, deploymentTraceId, guid, state) <> (ExecutionTrace.tupled, ExecutionTrace.unapply)
  }

  val executionTraceQuery = TableQuery[ExecutionTraceTable]

  def addTo(db: Database, deploymentTrace: DeploymentTrace): Future[Long] = {
    addToDeploymentTrace(db, deploymentTrace.id.get)
  }

  def addToDeploymentTrace(db: Database, deploymentTraceId: Long): Future[Long] = {
    val execTrace = ExecutionTrace(None, deploymentTraceId, "", ExecutionState.pending)
    db.run((executionTraceQuery returning executionTraceQuery.map(_.id)) += execTrace)
  }

  def findExecutionTraceById(db: Database, id: Long): Future[Option[ExecutionTrace]] = {
    db.run(executionTraceQuery.filter(_.id === id).result).map(_.headOption)
  }
}


@Singleton
class ExecutionTraceBinding @Inject()(val profile: JdbcProfile) extends ExecutionTraceBinder
  with DeploymentTraceBinder with DeploymentRequestBinder with ProfileProvider
