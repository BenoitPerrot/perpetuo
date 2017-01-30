package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{DeploymentRequest, Product}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DeploymentRequestBinder extends TableBinder {
  this: ProductBinder with DbContextProvider =>

  import dbContext.driver.api._

  class DeploymentRequestTable(tag: Tag) extends Table[DeploymentRequest](tag, "deployment_request") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    // The intent
    def productId = column[Int]("product_id")
    protected def fk = foreignKey(productId, productQuery)(_.id)
    def version = column[String]("version", O.SqlType("nchar(64)"))
    def target = column[String]("target", O.SqlType("nvarchar(max)"))

    // The details
    def comment = column[String]("comment", O.SqlType("nvarchar(256)"))
    def creator = column[String]("creator", O.SqlType("nchar(64)"))
    def creationDate = column[java.sql.Timestamp]("creation_date")

    def * = (id.?, productId, version, target, comment, creator, creationDate) <> (DeploymentRequest.tupled, DeploymentRequest.unapply)
  }

  val deploymentRequestQuery = TableQuery[DeploymentRequestTable]

  def insert(d: DeploymentRequest): Future[Long] = {
    dbContext.db.run((deploymentRequestQuery returning deploymentRequestQuery.map(_.id)) += d)
  }

  def findDeploymentRequestById(id: Long): Future[Option[DeploymentRequest]] = {
    dbContext.db.run(deploymentRequestQuery.filter(_.id === id).result).map(_.headOption)
  }

  def findDeploymentRequestByIdAndProduct(id: Long): Future[Option[(DeploymentRequest, Product)]] = {
    dbContext.db.run((deploymentRequestQuery join productQuery on (_.productId === _.id) filter (_._1.id === id)).result).map(_.headOption)
  }
}
