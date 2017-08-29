package com.criteo.perpetuo.engine

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dao.{DbBinding, UnknownProduct}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._

import scala.collection.JavaConverters._
import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object Action extends Enumeration {
  type Kind = Value

  val applyFirst = Value("start")
  val applyAgain = Value
  val rollback = Value
}


@Singleton
class Engine @Inject()(val dbBinding: DbBinding) {

  private val plugins = new Plugins(dbBinding)
  private val operationStarter = new OperationStarter(dbBinding)

  def getProductNames: Future[Seq[String]] =
    dbBinding.getProductNames

  def insertProduct(productName: String): Future[Product] =
    dbBinding.insertProduct(productName)

  // TODO: move out of Engine? <<
  def suggestVersions(productName: String): Seq[String] =
    plugins.externalData.suggestVersions(productName).asScala

  def validateVersion(productName: String, productVersion: String): Seq[String] = {
    plugins.externalData.validateVersion(productName, productVersion).asScala
  }
  // >>

  def findDeepDeploymentRequestById(deploymentRequestId: Long): Future[Option[DeepDeploymentRequest]] =
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId)

  def createDeploymentRequest(attrs: DeploymentRequestAttrs, immediateStart: Boolean): Future[Map[String, Any]] = {
    if (AppConfig.transition(attrs.productName) && !immediateStart) {
      dbBinding.findProductByName(attrs.productName)
        .map(_.map(DeepDeploymentRequest(0, _, attrs.version, attrs.target, attrs.comment, attrs.creator, attrs.creationDate))
          .getOrElse {
            throw new UnknownProduct(attrs.productName)
          })
        .map(depReq => Map("ticketUrl" -> plugins.listener.onDeploymentRequestCreated(depReq, immediateStart)))
    }
    else {
      val futureDepReq = dbBinding.insertDeploymentRequest(attrs)

      futureDepReq.foreach(plugins.listener.onDeploymentRequestCreated(_, immediateStart))

      if (immediateStart)
        futureDepReq.foreach(req => startDeploymentRequest(req, attrs.creator, atCreation = true))

      futureDepReq.map(depReq => Map("id" -> depReq.id))
    }
  }

  private def startDeploymentRequest(req: DeepDeploymentRequest, initiatorName: String, atCreation: Boolean): Future[(OperationTrace, Int, Int)] =
    operationStarter
      .start(plugins.dispatcher, req, Operation.deploy, initiatorName)
      .map { case (op, started, failed) =>
        Future(plugins.listener.onDeploymentRequestStarted(req, started, failed, atCreation))
        if (started == 0)
          closeOperation(op, req)
        (op, started, failed)
      }

  private def closeOperation(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Future[Boolean] = {
    dbBinding.closeOperationTrace(operationTrace.id).map { closingSuccess =>
      if (closingSuccess)
        dbBinding.isOperationSuccessful(operationTrace.id).foreach { succeeded =>
          assert(succeeded.isDefined, s"Operation #${operationTrace.id} doesn't exist or is not closed")
          plugins.listener.onOperationClosed(operationTrace, deploymentRequest, succeeded.get)
        }
      closingSuccess
    }
  }

  def isStarted(deploymentRequestId: Long): Future[Boolean] =
    dbBinding.isStarted(deploymentRequestId)

  def getConflictReason(deploymentRequestId: Long, isStarted: Boolean): Future[Option[String]] = {
    // todo: once there is no more * in TargetStatus table, we can allow successive rollbacks:
    //if (isStarted)
    //  dbBinding.findTargetAtomNotActionableBy(deploymentRequestId).map(
    //    _.map(targetAtom => s"the target `$targetAtom` (at least) is in a conflicting state")
    //  )
    //else
    dbBinding.isOutdated(deploymentRequestId).map(if (_) Some("a newer one has already been applied") else None)
  }

  def getActionsIf(isStarted: Boolean): Seq[Action.Kind] = {
    if (isStarted) Seq(Action.applyAgain) else Seq(Action.applyFirst)
  }

  def startDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[(OperationTrace, Int, Int)]] = {
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId).flatMap(
      _.map { req =>
        startDeploymentRequest(req, initiatorName, atCreation = false).map(Some(_))
      }.getOrElse(Future.successful(None))
    )
  }

  def deployAgain(deploymentRequestId: Long, initiatorName: String): Future[Option[OperationTrace]] = {
    dbBinding.findDeepDeploymentRequestAndSpecs(deploymentRequestId).flatMap(
      _.map { case (deploymentRequest, executionSpecs) =>
        operationStarter
          .deployAgain(plugins.dispatcher, deploymentRequest, executionSpecs, initiatorName)
          .map { case (operationTrace, started, failed) =>
            Future(plugins.listener.onDeploymentRequestRetried(deploymentRequest, started, failed))
            if (started == 0)
              closeOperation(operationTrace, deploymentRequest)
            Some(operationTrace)
          }
      }.getOrElse(Future.successful(None))
    )
  }

  def rollbackDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[OperationTrace]] =
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId).flatMap(
      _.map { depReq =>
        operationStarter
          .rollbackOperation(plugins.dispatcher, depReq, initiatorName)
          .map { case (operationTrace, started, failed) =>
            Future(plugins.listener.onDeploymentRequestRolledBack(depReq, started, failed))
            if (started == 0)
              closeOperation(operationTrace, depReq)
            Some(operationTrace)
          }
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

          val statusMapUpdate =
            if (statusMap.isEmpty)
              Future.successful(false)
            else
              dbBinding.updateOperationTrace(op.id, op.partialUpdate(statusMap))

          // TODO: don't insert, always update a pre-inserted status, wrt the precedence of statuses (DREDD-174)
          val statusMapToExecution = dbBinding.insertTargetStatuses(execTrace.executionId, statusMap).map(_ => true)

          Future.sequence(Seq(statusMapUpdate, statusMapToExecution))
            .flatMap(_ => dbBinding.hasOpenExecutionTracesForOperation(op.id))
            .flatMap { hasOpenExecutions =>
              if (hasOpenExecutions)
                Future.successful(Some(false))
              else
                dbBinding.findDeepDeploymentRequestById(op.deploymentRequestId).flatMap { depReq =>
                  closeOperation(op, depReq.get).map(Some(_))
                }
            }
        }
      }
      else
      // FIXME: update failed; raise an exception?
        Future.successful(None)
    }
  }

  def findDeepDeploymentRequestAndExecutions(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, SortedMap[Long, (Iterable[ExecutionTrace], Iterable[TargetStatus])])]] =
    dbBinding.findDeepDeploymentRequestAndExecutions(deploymentRequestId)

  def queryDeepDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[(DeepDeploymentRequest, SortedMap[Long, ArrayBuffer[ExecutionTrace]])]] =
    dbBinding.deepQueryDeploymentRequests(where, orderBy, limit, offset)

}
