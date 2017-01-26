package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model.{Operation, OperationTrace, TargetStatus}
import spray.json.{JsNumber, JsObject, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait OperationTraceBinder extends TableBinder {
  this: DeploymentRequestBinder with DbContextProvider =>

  import dbContext.driver.api._

  private implicit lazy val operationMapper = MappedColumnType.base[Operation, Short](
    op => op.id.toShort,
    short => Operation(short.toInt)
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

  def addToDeploymentRequest(requestId: Long, operation: Operation): Future[Long] = {
    val operationTrace = OperationTrace(None, requestId, operation)
    dbContext.db.run((operationTraceQuery returning operationTraceQuery.map(_.id)) += operationTrace)
  }

  def findOperationTraceById(id: Long): Future[Option[OperationTrace]] = {
    dbContext.db.run(operationTraceQuery.filter(_.id === id).result).map(_.headOption)
  }

  def findOperationTracesByDeploymentRequest(deploymentRequestId: Long): Future[Seq[OperationTrace]] = {
    dbContext.db.run(operationTraceQuery.filter(_.deploymentRequestId === deploymentRequestId).result)
  }

  def updateOperationTrace(id: Long, targetStatus: TargetStatus.MapType): Future[Unit] = {
    dbContext.db.run(operationTraceQuery.filter(_.id === id).map(_.targetStatus).update(targetStatus))
      .map(count => assert(count == 1))
  }
}
