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
                                                state: Option[DeploymentRequestState.Code],
                                                stateStamp: Int,
                                                autoRevert: Boolean) {
  def toDeploymentRequest(product: ProductRecord) =
    DeploymentRequest(id.get, product.toProduct, version.toModel, comment.toString, creator.toString, creationDate, state, stateStamp, autoRevert)
}


trait DeploymentRequestBinder extends TableBinder {
  this: ProductBinder with DbContextProvider =>

  import dbContext.profile.api._

  protected implicit lazy val stateCodeMapper = MappedColumnType.base[DeploymentRequestState.Code, Short](
    code => code.id.toShort,
    short => DeploymentRequestState(short.toInt)
  )

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

    def state = column[Option[DeploymentRequestState.Code]]("state")
    def stateStamp = column[Int]("state_stamp")
    def autoRevert = column[Boolean]("auto_revert")

    def * = (id.?, productId, version, comment, creator, creationDate, state, stateStamp, autoRevert) <> (DeploymentRequestRecord.tupled, DeploymentRequestRecord.unapply)
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

  def abandoningDeploymentRequest(id: Long): DBIOAction[Int, NoStream, Effect.Write] =
    deploymentRequestQuery.filter(_.id === id).map(_.state).update(Some(DeploymentRequestState.abandoned))

  /**
    * Update the state and possibly increment the state stamp.
    * WARNING: It does not handle concurrent incrementing updates.
    */
  def updatingDeploymentRequestState(id: Long, state: DeploymentRequestState.Code, incrementStateStamp: Boolean): DBIOAction[Int, NoStream, Effect.Write with Effect.Read with Effect.Transactional] = {
    val depReq = deploymentRequestQuery.filter(_.id === id)
    val updateState = depReq.map(_.state).update(Some(state))
    if (incrementStateStamp)
      updateState
        .andThen(depReq.map(_.stateStamp).result.head)
        .flatMap(stamp => depReq.map(_.stateStamp).update(stamp + 1))
        .transactionally
    else
      updateState
  }
}
