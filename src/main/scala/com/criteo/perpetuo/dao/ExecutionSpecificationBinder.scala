package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{ExecutionSpecification, Version}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

private[dao] case class ExecutionSpecificationRecord(id: Option[Long],
                                                     version: Version,
                                                     specificParameters: String) {
  def toExecutionSpecification: ExecutionSpecification = {
    ExecutionSpecification(id.get, version, specificParameters)
  }
}


trait ExecutionSpecificationBinder extends TableBinder {
  this: DbContextProvider =>

  import dbContext.profile.api._

  class ExecutionSpecificationTable(tag: Tag) extends Table[ExecutionSpecificationRecord](tag, "execution_specification") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def version = column[Version]("version", O.SqlType(s"nvarchar(${Version.maxSize})"))
    def specificParameters = column[String]("specific_parameters", O.SqlType("nvarchar(16000)"))

    def * = (id.?, version, specificParameters) <> (ExecutionSpecificationRecord.tupled, ExecutionSpecificationRecord.unapply)
  }

  val executionSpecificationQuery: TableQuery[ExecutionSpecificationTable] = TableQuery[ExecutionSpecificationTable]

  def insertingExecutionSpecification(specificParameters: String, version: Version): DBIOAction[ExecutionSpecification, NoStream, Effect.Write] =
    ((executionSpecificationQuery returning executionSpecificationQuery.map(_.id)) += ExecutionSpecificationRecord(None, version, specificParameters)).map(
      ExecutionSpecification(_, version, specificParameters)
    )

  def insertExecutionSpecification(specificParameters: String, version: Version): Future[ExecutionSpecification] =
    dbContext.db.run(insertingExecutionSpecification(specificParameters, version))

  def findExecutionSpecificationById(executionSpecificationId: Long): Future[Option[ExecutionSpecification]] =
    dbContext.db.run(executionSpecificationQuery.filter(_.id === executionSpecificationId).result)
      .map(_.headOption.map(_.toExecutionSpecification))
}
