package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{ExecutionSpecification, Version}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private[dao] case class ExecutionSpecificationRecord(id: Option[Long],
                                                     operationTraceId: Long,
                                                     version: Version,
                                                     specificParameters: String) {
  def toExecutionSpecification: ExecutionSpecification = {
    ExecutionSpecification(id.get, version, specificParameters)
  }
}


trait ExecutionSpecificationBinder extends TableBinder {
  this: DbContextProvider =>

  import dbContext.driver.api._

  class ExecutionSpecificationTable(tag: Tag) extends Table[ExecutionSpecificationRecord](tag, "execution_specification") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def operationTraceId = column[Long]("operation_trace_id")

    def version = column[Version]("version", O.SqlType(s"nvarchar(${Version.maxSize})"))
    def specificParameters = column[String]("specific_parameters", O.SqlType("nvarchar(16000)"))

    def * = (id.?, operationTraceId, version, specificParameters) <> (ExecutionSpecificationRecord.tupled, ExecutionSpecificationRecord.unapply)
  }

  val executionSpecificationQuery: TableQuery[ExecutionSpecificationTable] = TableQuery[ExecutionSpecificationTable]

  def insertExecutionSpecification(specificParameters: String, version: Version): Future[ExecutionSpecification] = {
    val record = ExecutionSpecificationRecord(None, 42, version, specificParameters)
    dbContext.db.run((executionSpecificationQuery returning executionSpecificationQuery.map(_.id)) += record).map(
      ExecutionSpecification(_, version, specificParameters)
    )
  }

  def findExecutionSpecificationById(executionSpecificationId: Long): Future[Option[ExecutionSpecification]] =
    dbContext.db.run(executionSpecificationQuery.filter(_.id === executionSpecificationId).result)
      .map(_.headOption.map(_.toExecutionSpecification))
}
