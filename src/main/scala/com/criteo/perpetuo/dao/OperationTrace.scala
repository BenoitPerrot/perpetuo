package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.dao.enums.{TargetStatus, Operation => OperationType}
import slick.driver.JdbcProfile
import spray.json.{JsNumber, JsObject, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class OperationTrace(id: Option[Long],
                          deploymentRequestId: Long,
                          operation: Operation,
                          targetStatus: TargetStatus.MapType)


trait OperationTraceBinder extends TableBinder {
  this: DeploymentRequestBinder with ProfileProvider =>

  import profile.api._

  private implicit lazy val operationMapper = MappedColumnType.base[Operation, Short](
    op => op.id.toShort,
    short => OperationType(short.toInt)
  )
  private implicit lazy val targetStatusMapper = MappedColumnType.base[TargetStatus.MapType, String](
    obj => JsObject(obj map { case (k, v) => (k, JsNumber(v.id)) }).toString,
    str => str.parseJson.asJsObject.fields map { case (k, JsNumber(v)) => (k, TargetStatus(v.value.toInt)) }
  )

  class OperationTraceTable(tag: Tag) extends Table[OperationTrace](tag, "operation_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def operation = column[Operation]("operation")
    def targetStatus = column[TargetStatus.MapType]("target_status", O.SqlType("nvarchar(max)"))

    def * = (id.?, deploymentRequestId, operation, targetStatus) <> (OperationTrace.tupled, OperationTrace.unapply)
  }

  val operationTraceQuery = TableQuery[OperationTraceTable]

  def addTo(db: Database, request: DeploymentRequest, operation: Operation): Future[Long] = {
    addToDeploymentRequest(db, request.id.get, operation)
  }

  def addToDeploymentRequest(db: Database, requestId: Long, operation: Operation): Future[Long] = {
    val trace = OperationTrace(None, requestId, operation, Map())
    db.run((operationTraceQuery returning operationTraceQuery.map(_.id)) += trace)
  }

  def findOperationTraceById(db: Database, id: Long): Future[Option[OperationTrace]] = {
    db.run(operationTraceQuery.filter(_.id === id).result).map(_.headOption)
  }

  def update(db: Database, id: Long, targetStatus: TargetStatus.MapType): Future[Int] = {
    db.run(operationTraceQuery.filter(_.id === id).map(_.targetStatus).update(targetStatus))
  }
}


@Singleton
class OperationTraceBinding @Inject()(val profile: JdbcProfile) extends OperationTraceBinder
  with DeploymentRequestBinder with ProfileProvider
