package com.criteo.perpetuo.dao

import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


private[dao] case class DeploymentRequestRecord(id: Option[Long],
                                                productId: Int,
                                                version: Version,
                                                target: String,
                                                comment: String, // Not an `Option` because it's easier to consider that no comment <=> empty
                                                creator: String,
                                                creationDate: java.sql.Timestamp) {
  def toShallowDeploymentRequest: ShallowDeploymentRequest = {
    ShallowDeploymentRequest(id.get, productId, version, target, comment, creator, creationDate)
  }

  def toDeepDeploymentRequest(product: ProductRecord): DeepDeploymentRequest = {
    DeepDeploymentRequest(id.get, product.toProduct, version, target, comment, creator, creationDate)
  }
}


trait DeploymentRequestBinder extends TableBinder {
  this: ProductBinder with DbContextProvider =>

  import dbContext.driver.api._

  class DeploymentRequestTable(tag: Tag) extends Table[DeploymentRequestRecord](tag, "deployment_request") {
    def id = column[Long]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    // The intent
    def productId = column[Int]("product_id")
    protected def fk = foreignKey(productId, productQuery)(_.id)
    def version = column[Version]("version", O.SqlType(s"nvarchar(${Version.maxSize})"))
    def target = column[String]("target", O.SqlType("nvarchar(8000)")) // TODO: remove after migrating to deployment-plan-step

    // The details
    def comment = column[String]("comment", O.SqlType("nvarchar(4000)"))
    def creator = column[String]("creator", O.SqlType(s"nvarchar(${User.maxSize})"))
    def creationDate = column[java.sql.Timestamp]("creation_date")
    protected def creationIdx = index(creationDate)

    def * = (id.?, productId, version, target, comment, creator, creationDate) <> (DeploymentRequestRecord.tupled, DeploymentRequestRecord.unapply)
  }

  val deploymentRequestQuery = TableQuery[DeploymentRequestTable]

  def insertDeploymentRequest(d: DeploymentRequestAttrs): Future[DeepDeploymentRequest] = {
    // find the product to which the corresponding foreign key is pointing to
    findProductByName(d.productName).map(_.getOrElse {
      throw new UnknownProduct(d.productName)
    }).flatMap { product =>
      val record = DeploymentRequestRecord(None, product.id, d.version, d.target, d.comment, d.creator, d.creationDate)
      dbContext.db.run((deploymentRequestQuery returning deploymentRequestQuery.map(_.id)) += record).map { id =>
        val ret = DeepDeploymentRequest(id, product, d.version, d.target, d.comment, d.creator, d.creationDate)
        ret.copyParsedTargetCacheFrom(d)
        ret
      }
    }
  }

  def deploymentRequestExists(id: Long): Future[Boolean] = {
    dbContext.db.run(deploymentRequestQuery.filter(_.id === id).exists.result)
  }

  def findDeepDeploymentRequestById(id: Long): Future[Option[DeepDeploymentRequest]] = {
    dbContext.db.run((deploymentRequestQuery join productQuery on (_.productId === _.id) filter (_._1.id === id)).result)
      .map(_.headOption.map {
        case (req, prod) => req.toDeepDeploymentRequest(prod)
      })
  }

  def updateDeploymentRequestComment(id: Long, comment: String): Future[Boolean] = {
    dbContext.db.run(deploymentRequestQuery.filter(_.id === id).map(_.comment).update(comment))
      .map {
        count =>
          assert(count <= 1)
          count == 1
      }
  }
}


class UnknownProduct(val productName: String) extends RuntimeException(s"Unknown product `$productName`")
