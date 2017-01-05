package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao.enums.{Operation, TargetStatus}
import slick.driver.JdbcProfile
import spray.json.{JsNumber, JsObject, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class DeploymentTrace(id: Option[Long],
                           deploymentRequestId: Long,
                           operation: Operation.Type,
                           targetStatus: TargetStatus.MapType)


trait DeploymentTraceBinder extends TableBinder {
  this: DeploymentRequestBinder with ProfileProvider =>

  import profile.api._

  private implicit lazy val operationMapper = MappedColumnType.base[Operation.Type, Short](
    op => op.id.toShort,
    short => Operation(short.toInt)
  )
  private implicit lazy val targetStatusMapper = MappedColumnType.base[TargetStatus.MapType, String](
    obj => JsObject(obj map { case (k, v) => (k, JsNumber(v.id)) }).toString,
    str => str.parseJson.asJsObject.fields map { case (k, JsNumber(v)) => (k, TargetStatus(v.value.toInt)) }
  )

  class DeploymentTraceTable(tag: Tag) extends Table[DeploymentTrace](tag, "deployment_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def operation = column[Operation.Type]("operation")
    def targetStatus = column[TargetStatus.MapType]("target_status", O.SqlType("nvarchar(max)"))

    def * = (id.?, deploymentRequestId, operation, targetStatus) <> (DeploymentTrace.tupled, DeploymentTrace.unapply)
  }

  val deploymentTraceQuery = TableQuery[DeploymentTraceTable]

  def addTo(db: Database, request: DeploymentRequest, operation: Operation.Type): Future[Long] = {
    addToDeploymentRequest(db, request.id.get, operation)
  }

  def addToDeploymentRequest(db: Database, requestId: Long, operation: Operation.Type): Future[Long] = {
    val trace = DeploymentTrace(None, requestId, operation, Map())
    db.run((deploymentTraceQuery returning deploymentTraceQuery.map(_.id)) += trace)
  }

  def findDeploymentTraceById(db: Database, id: Long): Future[Option[DeploymentTrace]] = {
    db.run(deploymentTraceQuery.filter(_.id === id).result).map(_.headOption)
  }

  def update(db: Database, id: Long, targetStatus: TargetStatus.MapType): Future[Int] = {
    db.run(deploymentTraceQuery.filter(_.id === id).map(_.targetStatus).update(targetStatus))
  }
}


@Singleton
class DeploymentTraceBinding @Inject()(val profile: JdbcProfile) extends DeploymentTraceBinder
  with DeploymentRequestBinder with ProfileProvider
