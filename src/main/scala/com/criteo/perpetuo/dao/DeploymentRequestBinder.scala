package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class DeploymentRequestRecord(id: Option[Long],
                                                productId: Int,
                                                version: VersionField,
                                                comment: Comment, // Not an `Option` because it's easier to consider that no comment <=> empty
                                                creator: UserName,
                                                creationDate: java.sql.Timestamp,
                                                state: Option[Short]) {
  def toDeploymentRequest(product: ProductRecord) =
    DeploymentRequest(id.get, product.toProduct, version.toModel, comment.toString, creator.toString, creationDate, state)
}


trait DeploymentRequestBinder extends TableBinder {
  this: ProductBinder with DbContextProvider =>

  import dbContext.profile.api._

  class DeploymentRequestTable(tag: Tag) extends Table[DeploymentRequestRecord](tag, "deployment_request") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    // The intent
    def productId = column[Int]("product_id")
    protected def fk = foreignKey(productId, productQuery)(_.id)
    def version = nvarchar[VersionField]("version")

    // The details
    def comment = nvarchar[Comment]("comment")
    def creator = nvarchar[UserName]("creator")
    def creationDate = column[java.sql.Timestamp]("creation_date")
    protected def creationIdx = index(creationDate)

    def state = column[Option[Short]]("state")

    def * = (id.?, productId, version, comment, creator, creationDate, state) <> (DeploymentRequestRecord.tupled, DeploymentRequestRecord.unapply)
  }

  val deploymentRequestQuery = TableQuery[DeploymentRequestTable]

  def deploymentRequestExists(id: Long): Future[Boolean] = {
    dbContext.db.run(deploymentRequestQuery.filter(_.id === id).exists.result)
  }

  def findingDeploymentRequestById(id: Long): DBIOAction[Option[DeploymentRequest], NoStream, Effect.Read] =
    deploymentRequestQuery
      .join(productQuery)
      .filter { case (deploymentRequest, product) =>
        deploymentRequest.id === id && deploymentRequest.productId === product.id
      }
      .result
      .map(_.headOption.map {
        case (req, prod) => req.toDeploymentRequest(prod)
      })

  def findDeploymentRequestById(id: Long): Future[Option[DeploymentRequest]] =
    dbContext.db.run(findingDeploymentRequestById(id))

  def updateDeploymentRequestComment(id: Long, comment: String): Future[Boolean] = {
    dbContext.db.run(deploymentRequestQuery.filter(_.id === id).map(_.comment).update(comment))
      .map {
        count =>
          assert(count <= 1)
          count == 1
      }
  }
}
