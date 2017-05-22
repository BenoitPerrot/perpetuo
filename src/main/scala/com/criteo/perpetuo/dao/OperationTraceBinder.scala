package com.criteo.perpetuo.dao

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model.{Operation, OperationTrace, Status, TargetAtomStatus}
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class OperationTraceRecord(id: Option[Long],
                                             deploymentRequestId: Long,
                                             operation: Operation,
                                             targetStatus: Status.TargetMap,
                                             creator: String,
                                             creationDate: java.sql.Timestamp) {
  def toOperationTrace: OperationTrace = {
    OperationTrace(id.get, deploymentRequestId, operation, targetStatus)
  }
}


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

    // todo: remove default values (they're for migration only)
    def creator = column[String]("creator", O.SqlType(s"nvarchar(${User.maxSize})"), O.Default("qabot"))
    def creationDate = column[java.sql.Timestamp]("creation_date", O.Default(new java.sql.Timestamp(0)))

    def * = (id.?, deploymentRequestId, operation, targetStatus, creator, creationDate) <> (OperationTraceRecord.tupled, OperationTraceRecord.unapply)
  }

  val operationTraceQuery = TableQuery[OperationTraceTable]

  def addToDeploymentRequest(requestId: Long, operation: Operation, creator: String): Future[Long] = {
    val operationTrace = OperationTraceRecord(None, requestId, operation, Map(), creator, new java.sql.Timestamp(System.currentTimeMillis))
    dbContext.db.run((operationTraceQuery returning operationTraceQuery.map(_.id)) += operationTrace)
  }

  def findOperationTracesByDeploymentRequest(deploymentRequestId: Long): Future[Seq[OperationTrace]] = {
    dbContext.db.run(operationTraceQuery.filter(_.deploymentRequestId === deploymentRequestId).sortBy(_.id).result).map(
      _.map(_.toOperationTrace)
    )
  }

  def updateOperationTrace(id: Long, targetStatus: Status.TargetMap): Future[Boolean] = {
    dbContext.db.run(operationTraceQuery.filter(_.id === id).map(_.targetStatus).update(targetStatus))
      .map(count => {
        assert(count <= 1)
        count == 1
      })
  }
}
