package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model.{Operation, Status, TargetAtomStatus}
import spray.json.{JsNumber, JsObject, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class OperationTraceRecord(id: Option[Long],
                                             deploymentRequestId: Long,
                                             operation: Operation,
                                             targetStatus: Status.TargetMap = Map())


trait OperationTraceBinder extends TableBinder {
  this: DeploymentRequestBinder with DbContextProvider =>

  import dbContext.driver.api._

  private implicit lazy val operationMapper = MappedColumnType.base[Operation, Short](
    op => op.id.toShort,
    short => Operation(short.toInt)
  )
  private implicit lazy val targetStatusMapper = MappedColumnType.base[Status.TargetMap, String](
    obj => JsObject(obj.mapValues { status => JsArray(JsNumber(status.code.id), JsString(status.detail)) }).compactPrint,
    str => str.parseJson.asJsObject.fields.mapValues { value =>
      val Vector(JsNumber(statusId), JsString(detail)) = value.asInstanceOf[JsArray].elements
      TargetAtomStatus(Status(statusId.toInt), detail)
    }
  )

  class OperationTraceTable(tag: Tag) extends Table[OperationTraceRecord](tag, "operation_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def operation = column[Operation]("operation")
    def targetStatus = column[Status.TargetMap]("target_status", O.SqlType("nvarchar(max)"))

    def * = (id.?, deploymentRequestId, operation, targetStatus) <> (OperationTraceRecord.tupled, OperationTraceRecord.unapply)
  }

  val operationTraceQuery = TableQuery[OperationTraceTable]

  def addToDeploymentRequest(requestId: Long, operation: Operation): Future[Long] = {
    val operationTrace = OperationTraceRecord(None, requestId, operation)
    dbContext.db.run((operationTraceQuery returning operationTraceQuery.map(_.id)) += operationTrace)
  }

  def findOperationTraceRecordsByDeploymentRequest(deploymentRequestId: Long): Future[Seq[OperationTraceRecord]] = {
    dbContext.db.run(operationTraceQuery.filter(_.deploymentRequestId === deploymentRequestId).result)
  }

  def updateOperationTrace(id: Long, targetStatus: Status.TargetMap): Future[Boolean] = {
    dbContext.db.run(operationTraceQuery.filter(_.id === id).map(_.targetStatus).update(targetStatus))
      .map(count => {
        assert(count <= 1)
        count == 1
      })
  }
}
