package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.Version


private[dao] case class ExecutionSpecificationRecord(id: Option[Long],
                                                     version: Version,
                                                     specificParameters: String)


trait ExecutionSpecificationBinder extends TableBinder {
  this: DbContextProvider =>

  import dbContext.driver.api._

  class ExecutionSpecificationTable(tag: Tag) extends Table[ExecutionSpecificationRecord](tag, "execution_specification") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def version = column[Version]("version", O.SqlType(s"nvarchar(${Version.maxSize})"))
    def specificParameters = column[String]("specific_parameters", O.SqlType("nvarchar(16000)"))

    def * = (id.?, version, specificParameters) <> (ExecutionSpecificationRecord.tupled, ExecutionSpecificationRecord.unapply)
  }

  val executionSpecificationQuery: TableQuery[ExecutionSpecificationTable] = TableQuery[ExecutionSpecificationTable]
}
