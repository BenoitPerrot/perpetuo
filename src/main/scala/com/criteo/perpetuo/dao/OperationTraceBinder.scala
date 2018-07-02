package com.criteo.perpetuo.dao

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.model._
import slick.sql.FixedSqlAction

import scala.concurrent.ExecutionContext.Implicits.global


private[dao] case class OperationTraceRecord(id: Option[Long],
                                             deploymentRequestId: Long,
                                             operation: Operation.Kind,
                                             creator: String,
                                             creationDate: java.sql.Timestamp,
                                             startingDate: Option[java.sql.Timestamp] = None,
                                             closingDate: Option[java.sql.Timestamp] = None) {
  def toOperationTrace(deploymentRequest: DeploymentRequest): DeepOperationTrace =
    DeepOperationTrace(id.get, deploymentRequest, operation, creator, creationDate, closingDate)
}


trait OperationTraceBinder extends TableBinder {
  this: DeploymentRequestBinder with ProductBinder with DbContextProvider =>

  import dbContext.profile.api._

  protected implicit lazy val operationMapper = MappedColumnType.base[Operation.Kind, Short](
    op => op.id.toShort,
    short => Operation(short.toInt)
  )

  class OperationTraceTable(tag: Tag) extends Table[OperationTraceRecord](tag, "operation_trace") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def deploymentRequestId = column[Long]("deployment_request_id")
    protected def fk = foreignKey(deploymentRequestId, deploymentRequestQuery)(_.id)

    def operation = column[Operation.Kind]("operation")

    def creator = column[String]("creator", O.SqlType(s"nvarchar(${User.maxSize})"))
    def creationDate = column[java.sql.Timestamp]("creation_date")
    protected def creationIdx = index(creationDate)
    def startingDate = column[Option[java.sql.Timestamp]]("starting_date")
    protected def startingIdx = index(startingDate)
    def closingDate = column[Option[java.sql.Timestamp]]("closing_date")
    protected def closingIdx = index(closingDate)

    def * = (id.?, deploymentRequestId, operation, creator, creationDate, startingDate, closingDate) <> (OperationTraceRecord.tupled, OperationTraceRecord.unapply)
  }

  val operationTraceQuery = TableQuery[OperationTraceTable]

  def insertOperationTrace(deploymentRequest: DeploymentRequest, operation: Operation.Kind, creator: String): DBIOAction[DeepOperationTrace, NoStream, Effect.Write] = {
    val date = new java.sql.Timestamp(System.currentTimeMillis)
    val operationTrace = OperationTraceRecord(None, deploymentRequest.id, operation, creator, date, Some(date))
    ((operationTraceQuery returning operationTraceQuery.map(_.id)) += operationTrace).map { id =>
      DeepOperationTrace(id, deploymentRequest, operation, creator, operationTrace.creationDate, operationTrace.closingDate)
    }
  }

  def countingOperationTraces(deploymentRequest: DeploymentRequest): FixedSqlAction[Int, dbContext.profile.api.NoStream, Effect.Read] =
    operationTraceQuery.filter(_.deploymentRequestId === deploymentRequest.id).length.result

  def closingOperationTrace(operationTrace: DeepOperationTrace): DBIOAction[Option[DeepOperationTrace], NoStream, Effect.Write] = {
    val now = Some(new java.sql.Timestamp(System.currentTimeMillis))
    operationTraceQuery
      .filter(op => op.id === operationTrace.id && op.startingDate.nonEmpty && op.closingDate.isEmpty)
      .map(_.closingDate)
      .update(now)
      .map(count => {
        assert(count <= 1)
        if (count == 1)
          Some(DeepOperationTrace(operationTrace.id, operationTrace.deploymentRequest, operationTrace.kind, operationTrace.creator, operationTrace.creationDate, closingDate = now))
        else
          None
      })
  }
}
