package com.criteo.perpetuo.engine

import java.sql.SQLException
import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dao.{DbBinding, UnknownProduct}
import com.criteo.perpetuo.dispatchers.Execution
import com.criteo.perpetuo.model.DeploymentRequestParser._
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
  private val execution =  new Execution(dbBinding)

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

  def createDeploymentRequest(creatorName: String, description: String, immediateStart: Boolean): Future[Map[String, Any]] = {
    // FIXME: move parsing to the caller
    val attrs = parse(description, creatorName)

    if (AppConfig.transition && !immediateStart) {
      dbBinding.findProductByName(attrs.productName)
        .map(_.map(DeploymentRequest(0, _, attrs.version, attrs.target, attrs.comment, attrs.creator, attrs.creationDate))
          .getOrElse {
            throw new UnknownProduct(attrs.productName)
          })
        .flatMap(plugins.hooks.onDeploymentRequestCreated(_, immediateStart, description))
        .map(ticketUrl => Map("ticketUrl" -> ticketUrl))
    }
    else {
      // first, log the user's general intent
      val futureDepReq = dbBinding.insert(attrs)
      // when the record is created, notify the corresponding hook
      futureDepReq.foreach(plugins.hooks.onDeploymentRequestCreated(_, immediateStart, description))

      if (immediateStart)
        futureDepReq.foreach(req => startDeploymentRequest(req, attrs.creator, immediately = true))

      futureDepReq.map(depReq => Map("id" -> depReq.id))
    }
  }

  private def startDeploymentRequest(req: DeploymentRequest, initiatorName: String, immediately: Boolean): Future[(Int, Int)] =
    execution
      .startOperation(plugins.dispatcher, req, Operation.deploy, initiatorName)
      .map { case (op, started, failed) =>
        plugins.hooks.onDeploymentRequestStarted(req, started, failed, immediately)
        (started, failed)
      }

  def startDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[Map[String, Any]]] =
    dbBinding.findDeploymentRequestByIdWithProduct(deploymentRequestId).map(_.map { req =>
      startDeploymentRequest(req, initiatorName, immediately = false)
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

  def updateExecutionTrace(id: Long, state: String, logHref: String, targetStatus: Map[String, Map[String, String]]): Future[Option[Unit]] = {
    val executionState =
      try {
        ExecutionState.withName(state)
      } catch {
        case _: NoSuchElementException => throw new IllegalArgumentException(s"Unknown state `$state`")
      }

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
        if (statusMap.nonEmpty) {
          // the execution trace has been updated, so it must exist!
          dbBinding.findExecutionTraceById(id).map(_.get).flatMap { execTrace =>
            val op = execTrace.operationTrace
            dbBinding.updateOperationTrace(op.id, op.partialUpdate(statusMap))
              .map { updated =>
                assert(updated)
                Some()
              }
          }
        }
        else
          Future.successful(Some())
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
