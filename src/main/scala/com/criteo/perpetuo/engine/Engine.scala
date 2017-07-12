package com.criteo.perpetuo.engine

import java.sql.SQLException

import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dao.UnknownProduct
import com.criteo.perpetuo.dispatchers.Execution
import com.criteo.perpetuo.model.DeploymentRequestParser._
import com.criteo.perpetuo.model._
import com.twitter.finatra.http.exceptions.{BadRequestException, ConflictException}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException
import spray.json.{DeserializationException, _}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class Engine(val execution: Execution) {

  private val plugins = new Plugins(execution.dbBinding)

  def getProductNames: Future[Seq[String]] =
    execution.dbBinding.getProductNames

  def insertProduct(productName: String): Future[Product] =
    execution.dbBinding.insert(productName)
      .recover {
        case e: SQLException if e.getMessage.contains("nique index") =>
          // there is no specific exception type if the name is already used but the error message starts with
          // - if H2: Unique index or primary key violation: "ix_product_name ON PUBLIC.""product""(""name"") VALUES ('my product', 1)"
          // - if SQLServer: Cannot insert duplicate key row in object 'dbo.product' with unique index 'ix_product_name'
          throw ConflictException(s"Name `$productName` is already used")
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
    execution.dbBinding.findDeploymentRequestByIdWithProduct(deploymentRequestId)

  def createDeploymentRequest(creatorName: String, description: String, immediateStart: Boolean): Future[Map[String, Any]] = {
    val attrs = try {
      parse(description, creatorName)
    }
    catch {
      case e: ParsingException => throw BadRequestException(e.getMessage)
    }

    if (AppConfig.transition && !immediateStart) {
      execution.dbBinding.findProductByName(attrs.productName)
        .map(_.map(DeploymentRequest(0, _, attrs.version, attrs.target, attrs.comment, attrs.creator, attrs.creationDate))
          .getOrElse {
            throw new UnknownProduct(attrs.productName)
          })
        .flatMap(plugins.hooks.onDeploymentRequestCreated(_, immediateStart, description))
        .map(ticketUrl => Map("ticketUrl" -> ticketUrl))
    }
    else {
      // first, log the user's general intent
      val futureDepReq = execution.dbBinding.insert(attrs)
      // when the record is created, notify the corresponding hook
      futureDepReq.foreach(plugins.hooks.onDeploymentRequestCreated(_, immediateStart, description))

      if (immediateStart) {
        futureDepReq.flatMap(depReq =>
          execution.startOperation(plugins.dispatcher, depReq, Operation.deploy, attrs.creator).map {
            case (started, failed) => (depReq, started, failed)
          }
        ).foreach { case (depReq, started, failed) =>
          plugins.hooks.onDeploymentRequestStarted(depReq, started, failed, immediately = true)
        }
      }

      futureDepReq
        .map(depReq => Map("id" -> depReq.id))
    }
  }

  def startDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[Map[String, Any]]] =
    execution.dbBinding.findDeploymentRequestByIdWithProduct(deploymentRequestId).map(_.map { req =>
      // done asynchronously
      execution.startOperation(plugins.dispatcher, req, Operation.deploy, initiatorName)
        .foreach { case (started, failed) =>
          plugins.hooks.onDeploymentRequestStarted(req, started, failed, immediately = false)
        }

      // returned synchronously
      Map("id" -> req.id)
    })

  def findOperationTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[OperationTrace]]] =
    execution.dbBinding.findOperationTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        execution.dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ExecutionTrace]]] =
    execution.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        execution.dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def updateExecutionTrace(id: Long, state: String, logHref: String, targetStatus: Map[String, Map[String, String]]): Future[Option[Unit]] = {
    val executionState =
      try {
        ExecutionState.withName(state)
      } catch {
        case _: NoSuchElementException => throw BadRequestException(s"Unknown state `$state`")
      }

    val statusMap =
      try {
        targetStatus.map { // don't use mapValues, as it gives a view (lazy generation, incompatible with error management here)
          case (k, obj) => (k, Status.targetMapJsonFormat.read(obj.toJson)) // yes it's crazy to use spray's case class deserializer
        }
      } catch {
        case e: DeserializationException => throw BadRequestException(e.getMessage)
      }

    val executionUpdate =
      if (logHref.nonEmpty)
        execution.dbBinding.updateExecutionTrace(id, logHref, executionState)
      else
        execution.dbBinding.updateExecutionTrace(id, executionState)

    if (statusMap.nonEmpty)
      executionUpdate.flatMap {
        if (_) {
          // the execution trace has been updated, so it must exist!
          execution.dbBinding.findExecutionTraceById(id).map(_.get).flatMap { execTrace =>
            val op = execTrace.operationTrace
            execution.dbBinding.updateOperationTrace(op.id, op.partialUpdate(statusMap))
              .map { updated =>
                assert(updated)
                Some()
              }
          }
        }
        else
          Future.successful(None)
      }
    else
      executionUpdate.map {
        if (_) Some() else None
      }
  }

  def getDeepDeploymentRequest(deploymentRequestId: Long): Future[Option[Map[String, Any]]] =
    execution.dbBinding.deepQueryDeploymentRequests(deploymentRequestId)

  def queryDeepDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[Map[String, Any]]] =
    execution.dbBinding.deepQueryDeploymentRequests(where, orderBy, limit, offset)

}
