package com.criteo.perpetuo.engine

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dao.{DbBinding, UnknownProduct}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

@Singleton
class Engine @Inject()(val dbBinding: DbBinding) {

  private val plugins = new Plugins(dbBinding)
  private val operationStarter = new OperationStarter(dbBinding)

  def getProductNames: Future[Seq[String]] =
    dbBinding.getProductNames

  def insertProduct(productName: String): Future[Product] =
    dbBinding.insert(productName)

  // TODO: move out of Engine? <<
  def suggestVersions(productName: String): Seq[String] =
    plugins.externalData.suggestVersions(productName).asScala

  def validateVersion(productName: String, productVersion: String): Seq[String] = {
    plugins.externalData.validateVersion(productName, productVersion).asScala
  }
  // >>

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

  private def startDeploymentRequest(req: DeploymentRequest, initiatorName: String, atCreation: Boolean): Future[(OperationTrace, Int, Int)] =
    operationStarter
      .start(plugins.dispatcher, req, Operation.deploy, initiatorName)
      .map { case (op, started, failed) =>
        Future(plugins.hooks.onDeploymentRequestStarted(req, started, failed, atCreation))
        if (started == 0)
          closeOperation(op, req)
        (op, started, failed)
      }

  private def closeOperation(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest): Future[Boolean] = {
    dbBinding.closeOperationTrace(operationTrace.id).map { closingSuccess =>
      if (closingSuccess)
        dbBinding.isOperationSuccessful(operationTrace.id).foreach { succeeded =>
          assert(succeeded.isDefined, s"Operation #${operationTrace.id} doesn't exist or is not closed")
          plugins.hooks.onOperationClosed(operationTrace, deploymentRequest, succeeded.get)
        }
      closingSuccess
    }
  }

  def startDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[(OperationTrace, Int, Int)]] =
    dbBinding.findDeploymentRequestByIdWithProduct(deploymentRequestId).flatMap(
      _.map { req =>
        startDeploymentRequest(req, initiatorName, atCreation = false).map(Some(_))
      }.getOrElse(Future.successful(None))
    )

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
  def updateExecutionTrace(id: Long, executionState: ExecutionState, logHref: String, statusMap: Map[String, TargetAtomStatus]): Future[Option[Boolean]] = {
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

          val statusMapToExecution = dbBinding.addToExecution(execTrace.executionId, statusMap).map(_ => true)

          Future.sequence(Seq(operationClosingAttempt, statusMapUpdate, statusMapToExecution)).map(x => {
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
