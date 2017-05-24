package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.Version


private[dao] case class ExecutionSpecificationRecord(id: Option[Long],
                                                     operationTraceId: Long,
                                                     version: Option[String],
                                                     specificParameters: String)


trait ExecutionSpecificationBinder extends TableBinder {
  this: OperationTraceBinder with DbContextProvider =>

  import dbContext.driver.api._

  class ExecutionSpecificationTable(tag: Tag) extends Table[ExecutionSpecificationRecord](tag, "execution_specification") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def operationTraceId = column[Long]("operation_trace_id")
    protected def fk = foreignKey(operationTraceId, operationTraceQuery)(_.id)

    def version = column[Option[String]]("version", O.SqlType(s"nvarchar(${Version.maxSize})"))
    def specificParameters = column[String]("specific_parameters", O.SqlType("nvarchar(4000)"))

    def * = (id.?, operationTraceId, version, specificParameters) <> (ExecutionSpecificationRecord.tupled, ExecutionSpecificationRecord.unapply)
  }

  val executionSpecificationQuery: TableQuery[ExecutionSpecificationTable] = TableQuery[ExecutionSpecificationTable]
}
