package com.criteo.perpetuo.dao


private[dao] case class ExecutionRecord(id: Option[Long],
                                        operationTraceId: Long,
                                        executionSpecificationId: Long)


trait ExecutionBinder extends TableBinder {
  this: OperationTraceBinder with ExecutionSpecificationBinder with DbContextProvider =>

  import dbContext.driver.api._

  class ExecutionTable(tag: Tag) extends Table[ExecutionRecord](tag, "execution") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def operationTraceId = column[Long]("operation_trace_id")
    protected def opFk = foreignKey(operationTraceId, operationTraceQuery)(_.id)
    def executionSpecificationId = column[Long]("execution_specification_id")
    protected def execFk = foreignKey(executionSpecificationId, executionSpecificationQuery)(_.id)
    protected def idx = index("ix_execution", (operationTraceId, executionSpecificationId), unique = true)

    def * = (id.?, operationTraceId, executionSpecificationId) <> (ExecutionRecord.tupled, ExecutionRecord.unapply)
  }

  val executionQuery: TableQuery[ExecutionTable] = TableQuery[ExecutionTable]
}