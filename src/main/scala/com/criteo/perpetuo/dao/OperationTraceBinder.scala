package com.criteo.perpetuo.dao

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.model._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class OperationTraceRecord(id: Option[Long],
                                             deploymentRequestId: Long,
                                             operation: Operation.Kind,
                                             targetStatus: Option[Status.TargetMap],
                                             creator: String,
                                             creationDate: java.sql.Timestamp,
                                             startingDate: Option[java.sql.Timestamp] = None,
                                             closingDate: Option[java.sql.Timestamp] = None) {
  def toOperationTrace: ShallowOperationTrace = {
    ShallowOperationTrace(id.get, deploymentRequestId, operation, creator, creationDate, closingDate, targetStatus.get)
  }
}


trait OperationTraceBinder extends TableBinder {
  this: DeploymentRequestBinder with ProductBinder with DbContextProvider =>

  import dbContext.driver.api._

  protected implicit lazy val operationMapper = MappedColumnType.base[Operation.Kind, Short](
    op => op.id.toShort,
    short => Operation(short.toInt)
  )
  protected implicit lazy val targetStatusMapper = MappedColumnType.base[Status.TargetMap, String](
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

    def operation = column[Operation.Kind]("operation")
    def targetStatus = column[Option[Status.TargetMap]]("target_status", O.SqlType("nvarchar(16000)")) // todo: remove after transition

    def creator = column[String]("creator", O.SqlType(s"nvarchar(${User.maxSize})"))
    def creationDate = column[java.sql.Timestamp]("creation_date")
    protected def creationIdx = index(creationDate)
    def startingDate = column[Option[java.sql.Timestamp]]("starting_date", O.Default(None)) // fixme: default for the migration only
    protected def startingIdx = index(startingDate)
    def closingDate = column[Option[java.sql.Timestamp]]("closing_date")
    protected def closingIdx = index(closingDate)

    def * = (id.?, deploymentRequestId, operation, targetStatus, creator, creationDate, startingDate, closingDate) <> (OperationTraceRecord.tupled, OperationTraceRecord.unapply)
  }

  val operationTraceQuery = TableQuery[OperationTraceTable]

  def insertOperationTrace(requestId: Long, operation: Operation.Kind, creator: String): Future[ShallowOperationTrace] = {
    val date = new java.sql.Timestamp(System.currentTimeMillis)
    val operationTrace = OperationTraceRecord(None, requestId, operation, Some(Map()), creator, date, Some(date))
    dbContext.db.run((operationTraceQuery returning operationTraceQuery.map(_.id)) += operationTrace).map { id =>
      ShallowOperationTrace(id, operationTrace.deploymentRequestId, operationTrace.operation, operationTrace.creator, operationTrace.creationDate, operationTrace.closingDate, operationTrace.targetStatus.get)
    }
  }

  def findOperationTracesByDeploymentRequest(deploymentRequestId: Long): Future[Seq[ShallowOperationTrace]] = {
    dbContext.db.run(operationTraceQuery.filter(_.deploymentRequestId === deploymentRequestId).sortBy(_.id).result).map(
      _.map(_.toOperationTrace)
    )
  }

  def updateOperationTrace(id: Long, targetStatus: Status.TargetMap): Future[Boolean] = {
    dbContext.db.run(operationTraceQuery.filter(_.id === id).map(_.targetStatus).update(Some(targetStatus)))
      .map(count => {
        assert(count <= 1)
        count == 1
      })
  }

  def closeOperationTrace(id: Long): Future[Boolean] = {
    dbContext.db.run(
      operationTraceQuery
        .filter(operationTrace => operationTrace.id === id && operationTrace.closingDate.isEmpty)
        .map(_.closingDate)
        .update(Some(new java.sql.Timestamp(System.currentTimeMillis)))
        .transactionally
    ).map(count => {
      assert(count <= 1)
      count == 1
    })
  }
}
