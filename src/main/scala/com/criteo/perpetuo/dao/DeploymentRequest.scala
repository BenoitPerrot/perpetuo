package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dispatchers.{DeploymentRequestParser, TargetExpr}
import slick.driver.JdbcProfile
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class DeploymentRequest(id: Option[Long],
                             productName: String,
                             version: String,
                             target: String,
                             reason: String, // Not an `Option` because it's easier to consider that no comment <=> empty
                             creator: String,
                             creationDate: java.sql.Timestamp) {

  // laziness of parsedTarget is handled by hand, to be able to duplicate the instance
  // and still benefit from an already parsed target without forcing it
  private var parsedTargetCache: Option[TargetExpr] = None
  def parsedTarget: TargetExpr = parsedTargetCache.getOrElse {
    val parsed = DeploymentRequestParser.parseTargetExpression(target.parseJson)
    parsedTargetCache = Some(parsed)
    parsed
  }
}


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
    def creator = column[String]("creator", O.SqlType("nchar(64)"))
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
