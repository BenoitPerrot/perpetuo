package com.criteo.perpetuo.engine

import java.sql.SQLException
import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dao.{DbBinding, UnknownProduct}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class ProductCreationConflict(productName: String, private val cause: Throwable = None.orNull)
  extends Exception(s"Name `$productName` is already used", cause)

@Singleton
class Engine @Inject()(val dbBinding: DbBinding) {

  private val plugins = new Plugins(dbBinding)
  private val operationStarter = new OperationStarter(dbBinding)

  def getProductNames: Future[Seq[String]] =
    dbBinding.getProductNames

  def insertProduct(productName: String): Future[Product] =
    dbBinding.insert(productName)
      .recover {
        case e: SQLException if e.getMessage.contains("nique index") =>
          // there is no specific exception type if the name is already used but the error message starts with
          // - if H2: Unique index or primary key violation: "ix_product_name ON PUBLIC.""product""(""name"") VALUES ('my product', 1)"
          // - if SQLServer: Cannot insert duplicate key row in object 'dbo.product' with unique index 'ix_product_name'
          throw ProductCreationConflict(productName, e)
      }

  def suggestVersions(productName: String): Seq[String] =
    plugins.externalData.suggestVersions(productName).asScala

  def validateVersion(productName: String, productVersion: String): Map[String, Any] = {
    val reasonsForInvalidity = plugins.externalData.validateVersion(productName, productVersion).asScala
    if (reasonsForInvalidity.isEmpty)
      Map("valid" -> true)
    else
      Map("valid" -> false, "reason" -> reasonsForInvalidity)
  }

  def findDeploymentRequestByIdWithProduct(deploymentRequestId: Long): Future[Option[DeploymentRequest]] =
    dbBinding.findDeploymentRequestByIdWithProduct(deploymentRequestId)

  def createDeploymentRequest(attrs: DeploymentRequestAttrs, immediateStart: Boolean): Future[Map[String, Any]] = {
    if (AppConfig.transition(attrs.productName) && !immediateStart) {
      dbBinding.findProductByName(attrs.productName)
        .map(_.map(DeploymentRequest(0, _, attrs.version, attrs.target, attrs.comment, attrs.creator, attrs.creationDate))
          .getOrElse {
            throw new UnknownProduct(attrs.productName)
          })
        .map(depReq => Map("ticketUrl" -> plugins.hooks.onDeploymentRequestCreated(depReq, immediateStart)))
    }
    else {
      // first, log the user's general intent
      val futureDepReq = dbBinding.insert(attrs)
      // when the record is created, notify the corresponding hook
      futureDepReq.foreach(plugins.hooks.onDeploymentRequestCreated(_, immediateStart))

      if (immediateStart)
        futureDepReq.foreach(req => startDeploymentRequest(req, attrs.creator, atCreation = true))

      futureDepReq.map(depReq => Map("id" -> depReq.id))
    }
  }

  private def startDeploymentRequest(req: DeploymentRequest, initiatorName: String, atCreation: Boolean): Future[(Int, Int)] =
    operationStarter
      .start(plugins.dispatcher, req, Operation.deploy, initiatorName)
      .map { case (op, started, failed) =>
        Future(plugins.hooks.onDeploymentRequestStarted(req, started, failed, atCreation))
        if (started == 0)
          closeOperation(op, req)
        (started, failed)
      }

  private def closeOperation(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest): Future[Boolean] = {
    dbBinding.closeOperationTrace(operationTrace.id).map { closingSuccess =>
      if (closingSuccess)
        Future(plugins.hooks.onOperationClosed(operationTrace, deploymentRequest, operationTrace.succeeded))
      closingSuccess
    }
  }

  def startDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[Map[String, Any]]] =
    dbBinding.findDeploymentRequestByIdWithProduct(deploymentRequestId).map(_.map { req =>
      startDeploymentRequest(req, initiatorName, atCreation = false)
      Map("id" -> req.id)
    })

  def findOperationTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[OperationTrace]]] =
    dbBinding.findOperationTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ExecutionTrace]]] =
    dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  /**
    * @return ultimately true when the linked OperationTrace has been closed by the update, false otherwise
    */
  def updateExecutionTrace(id: Long, executionState: ExecutionState, logHref: String, targetStatus: Map[String, Map[String, String]]): Future[Option[Boolean]] = {
    val statusMap =
      try {
        targetStatus.map { // don't use mapValues, as it gives a view (lazy generation, incompatible with error management here)
          case (k, obj) => (k, Status.targetMapJsonFormat.read(obj.toJson)) // yes it's crazy to use spray's case class deserializer
        }
      } catch {
        case e: DeserializationException => throw new IllegalArgumentException(e.getMessage)
      }

    val executionUpdate =
      if (logHref.nonEmpty)
        dbBinding.updateExecutionTrace(id, logHref, executionState)
      else
        dbBinding.updateExecutionTrace(id, executionState)

    executionUpdate.flatMap {
      if (_) {
        // the execution trace has been updated, so it must exist!
        dbBinding.findExecutionTraceById(id).map(_.get).flatMap { execTrace =>
          val op = execTrace.operationTrace

          val operationClosingAttempt =
            dbBinding.hasOpenExecutionTracesForOperation(op.id).flatMap { hasOpenExecutions =>
              if (hasOpenExecutions)
                Future.successful(false)
              else
                dbBinding.findDeploymentRequestByIdWithProduct(op.deploymentRequestId).flatMap { depReq =>
                  closeOperation(op, depReq.get)
                }
            }

          val statusMapUpdate =
            if (statusMap.isEmpty)
              Future.successful(false)
            else
              dbBinding.updateOperationTrace(op.id, op.partialUpdate(statusMap))

          Future.sequence(Seq(operationClosingAttempt, statusMapUpdate)).map(x => {
            Some(x.head)
          })
        }
      }
      else
      // FIXME: update failed; raise an exception?
        Future.successful(None)
    }
  }

  def getDeepDeploymentRequest(deploymentRequestId: Long): Future[Option[Map[String, Any]]] =
    dbBinding.deepQueryDeploymentRequests(deploymentRequestId)

  def queryDeepDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[Map[String, Any]]] =
    dbBinding.deepQueryDeploymentRequests(where, orderBy, limit, offset)

}
