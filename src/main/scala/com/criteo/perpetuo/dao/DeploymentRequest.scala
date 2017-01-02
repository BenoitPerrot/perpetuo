package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import slick.driver.JdbcProfile

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


trait DeploymentIntent {
  val productName: String
  val version: String
  val target: String
  val reason: String // Not an `Option` because it's easier to consider that no comment <=> empty
}


case class DeploymentRequest(id: Option[Long],
                             productName: String,
                             version: String,
                             target: String,
                             reason: String,
                             creator: String,
                             creationDate: java.sql.Timestamp) extends DeploymentIntent


trait DeploymentRequestBinder extends TableBinder {
  this: ProfileProvider =>

  import profile.api._

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
    def creator = column[String]("creator", O.SqlType("nvarchar(64)"))
    def creationDate = column[java.sql.Timestamp]("creation_date")

    def * = (id.?, productName, version, target, reason, creator, creationDate) <> (DeploymentRequest.tupled, DeploymentRequest.unapply)
  }

  val deploymentRequestQuery = TableQuery[DeploymentRequestTable]

  def insert(db: Database, d: DeploymentRequest): Future[Long] = {
    db.run((deploymentRequestQuery returning deploymentRequestQuery.map(_.id)) += d)
  }

  def findDeploymentRequestById(db: Database, id: Long): Future[Option[DeploymentRequest]] = {
    db.run(deploymentRequestQuery.filter(_.id === id).result).map(_.headOption)
  }
}


@Singleton
class DeploymentRequestBinding @Inject()(val profile: JdbcProfile) extends DeploymentRequestBinder
  with ProfileProvider
