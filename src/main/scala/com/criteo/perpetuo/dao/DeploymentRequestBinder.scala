package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.DeploymentRequest

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DeploymentRequestBinder extends TableBinder {
  this: DbContextProvider =>

  import dbContext.driver.api._

  class DeploymentRequestTable(tag: Tag) extends Table[DeploymentRequest](tag, "deployment_request") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    // The intent
    def productName = column[String]("product_name", O.SqlType("nchar(64)"))
    protected def idx = index(productName)
    def version = column[String]("version", O.SqlType("nchar(64)"))
    def target = column[String]("target", O.SqlType("nvarchar(max)"))

    // The details
    def reason = column[String]("reason", O.SqlType("nvarchar(256)"))
    def creator = column[String]("creator", O.SqlType("nchar(64)"))
    def creationDate = column[java.sql.Timestamp]("creation_date")

    def * = (id.?, productName, version, target, reason, creator, creationDate) <> (DeploymentRequest.tupled, DeploymentRequest.unapply)
  }

  val deploymentRequestQuery = TableQuery[DeploymentRequestTable]

  def insert(d: DeploymentRequest): Future[Long] = {
    dbContext.db.run((deploymentRequestQuery returning deploymentRequestQuery.map(_.id)) += d)
  }

  def findDeploymentRequestById(id: Long): Future[Option[DeploymentRequest]] = {
    dbContext.db.run(deploymentRequestQuery.filter(_.id === id).result).map(_.headOption)
  }
}
