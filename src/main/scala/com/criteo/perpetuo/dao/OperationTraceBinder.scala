package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model.{Operation, TargetStatus}
import spray.json.{JsNumber, JsObject, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class OperationTraceRecord(id: Option[Long],
                                             deploymentRequestId: Long,
                                             operation: Operation,
                                             targetStatus: TargetStatus.MapType = Map())


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

  class OperationTraceTable(tag: Tag) extends Table[OperationTraceRecord](tag, "operation_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def operation = column[Operation]("operation")
    def targetStatus = column[TargetStatus.MapType]("target_status", O.SqlType("nvarchar(max)"))

    def * = (id.?, deploymentRequestId, operation, targetStatus) <> (OperationTraceRecord.tupled, OperationTraceRecord.unapply)
  }

  val operationTraceQuery = TableQuery[OperationTraceTable]

  def addToDeploymentRequest(requestId: Long, operation: Operation): Future[Long] = {
    val operationTrace = OperationTraceRecord(None, requestId, operation)
    dbContext.db.run((operationTraceQuery returning operationTraceQuery.map(_.id)) += operationTrace)
  }

  def updateOperationTrace(id: Long, targetStatus: TargetStatus.MapType): Future[Boolean] = {
    dbContext.db.run(operationTraceQuery.filter(_.id === id).map(_.targetStatus).update(targetStatus))
      .map(count => {
        assert(count <= 1)
        count == 1
      })
  }
}
