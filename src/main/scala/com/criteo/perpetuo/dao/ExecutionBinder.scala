package com.criteo.perpetuo.dao

import slick.sql.FixedSqlAction

import scala.concurrent.Future


private[dao] case class ExecutionRecord(id: Option[Long],
                                        operationTraceId: Long,
                                        executionSpecificationId: Long)


trait ExecutionBinder extends TableBinder {
  this: OperationTraceBinder with ExecutionSpecificationBinder with DbContextProvider =>

  import dbContext.profile.api._

  class ExecutionTable(tag: Tag) extends Table[ExecutionRecord](tag, "execution") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def operationTraceId = column[Long]("operation_trace_id")
    protected def opFk = foreignKey(operationTraceId, operationTraceQuery)(_.id)
    def executionSpecificationId = column[Long]("execution_specification_id")
    protected def execFk = foreignKey(executionSpecificationId, executionSpecificationQuery)(_.id)
    protected def idx = index("ix_execution_operation_trace_id_execution_specification_id", (operationTraceId, executionSpecificationId), unique = true)

    def * = (id.?, operationTraceId, executionSpecificationId) <> (ExecutionRecord.tupled, ExecutionRecord.unapply)
  }

  val executionQuery: TableQuery[ExecutionTable] = TableQuery[ExecutionTable]

  def insertExecution(operationTraceId: Long, executionSpecificationId: Long): FixedSqlAction[Long, NoStream, Effect.Write] = {
    (executionQuery returning executionQuery.map(_.id)) += ExecutionRecord(None, operationTraceId, executionSpecificationId)
  }

  def findExecutionIdsByOperationTrace(operationTraceId: Long): Future[Seq[Long]] =
    dbContext.db.run(executionQuery.filter(_.operationTraceId === operationTraceId).map(_.id).result)

  // for tests only
  def findExecutionSpecIdsByOperationTrace(operationTraceId: Long): Future[Seq[Long]] =
    dbContext.db.run(
      executionQuery
        .filter { execution => execution.operationTraceId === operationTraceId }
        .map { execution => execution.executionSpecificationId }
        .result
    )
}
