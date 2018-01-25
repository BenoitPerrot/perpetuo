package com.criteo.perpetuo.engine

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.auth.Permissions
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.{DbBinding, UnknownProduct}
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try


object OperationStatus extends Enumeration {
  val inProgress = Value
  val flopped = Value
  val failed = Value
  val succeeded = Value
}

abstract class RejectingError extends RuntimeException {
  val msg: String
  val detail: Map[String, _]

  def copy(newMessage: String): RejectingError

  override def getMessage: String = msg + detail.map {
    case (k, v: Iterable[_]) => s"; $k: ${v.mkString(", ")}"
    case (k, v) => s"; $k: $v"
  }.mkString("")
}

case class UnavailableAction(msg: String, detail: Map[String, _] = Map()) extends RejectingError {
  def copy(newMessage: String) = UnavailableAction(newMessage, detail)
}

case class MissingInfo(msg: String, required: String) extends RejectingError {
  val detail: Map[String, String] = Map("required" -> required)

  def copy(newMessage: String) = MissingInfo(newMessage, required)
}

case class Conflict(msg: String, conflicts: Iterable[_] = Seq()) extends RejectingError {
  val detail: Map[String, Iterable[_]] = if (conflicts.nonEmpty) Map("conflicts" -> conflicts) else Map()

  def copy(newMessage: String) = Conflict(newMessage, conflicts)
}


@Singleton
class Engine @Inject()(val dbBinding: DbBinding,
                       val targetResolver: TargetResolver,
                       val targetDispatcher: TargetDispatcher,
                       val permissions: Permissions,
                       val listeners: Seq[Listener]) {

  val config = AppConfigProvider.config

  private val withTransactions = !Try(config.getBoolean("noTransactions")).getOrElse(false)

  private val operationStarter = new OperationStarter(dbBinding)

  // todo: remove once new workflow is completely in place <<
  lazy val productsExcludedFromNewWorkflow: Seq[String] = config.tryGet("productsExcludedFromNewWorkflow").getOrElse(Seq())

  def isCoveredByOldWorkflow(productName: String): Boolean =
    productsExcludedFromNewWorkflow.contains(productName)

  // >>

  def getProductNames: Future[Seq[String]] =
    dbBinding.getProductNames

  def insertProduct(productName: String): Future[Product] =
    dbBinding.insertProduct(productName)

  def findDeepDeploymentRequestById(deploymentRequestId: Long): Future[Option[DeepDeploymentRequest]] =
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId)

  def createDeploymentRequest(attrs: DeploymentRequestAttrs, immediateStart: Boolean): Future[Map[String, Any]] = {
    // todo: replace that by the creation of all the related records when the first deploy operation will be created simultaneously
    targetDispatcher.freezeParameters(attrs.productName, attrs.version)
    operationStarter.expandTarget(targetResolver, attrs.productName, attrs.version, attrs.parsedTarget)

    // todo: remove once new workflow is completely in place <<
    if (isCoveredByOldWorkflow(attrs.productName) && !immediateStart) {
      dbBinding.findProductByName(attrs.productName)
        .map(_.map(DeepDeploymentRequest(0, _, attrs.version, attrs.target, attrs.comment, attrs.creator, attrs.creationDate))
          .getOrElse {
            throw new UnknownProduct(attrs.productName)
          })
        .map(depReq => Map("ticketUrl" -> listeners.map(_.onDeploymentRequestCreated(depReq, immediateStart)).mkString("")))
    } // >>
    else {
      val futureDepReq = dbBinding.insertDeploymentRequest(attrs)

      futureDepReq.foreach { deploymentRequest =>
        // todo: onDeploymentRequestCreated returning an extra comment feels wrong, probably it should not
        val moreComments = listeners.map(_.onDeploymentRequestCreated(deploymentRequest, immediateStart)).filter(_ != null)
        (if (moreComments.nonEmpty) {
          val allComments = if (deploymentRequest.comment.nonEmpty) Seq(deploymentRequest.comment) ++ moreComments else moreComments
          dbBinding.updateDeploymentRequestComment(deploymentRequest.id, allComments.mkString("\n"))
        } else {
          Future.successful()
        }).foreach { _ =>
          if (immediateStart)
            startDeploymentRequest(deploymentRequest, attrs.creator, atCreation = true)
        }
      }

      futureDepReq.map(depReq => Map("id" -> depReq.id))
    }
  }

  private def getOperationLockName(deploymentRequest: DeploymentRequest) =
    s"Operating on ${deploymentRequest.id}"

  private def startOperation(deploymentRequest: DeepDeploymentRequest,
                             operationStart: => Future[(ShallowOperationTrace, Int, Int)],
                             onOperationStartedListeners: Seq[(DeepDeploymentRequest, Int, Int) => Unit]): Future[ShallowOperationTrace] =
    dbBinding.tryAcquireLocks(Seq(getOperationLockName(deploymentRequest)), deploymentRequest.id).flatMap { alreadyRunning =>
      if (alreadyRunning.nonEmpty)
        throw Conflict("Cannot be processed for the moment because another operation is running for the same deployment request")

        val opStart = try {
          operationStart
        } catch {
          case _: Throwable => Future.failed(new Exception)
        }
        opStart
          .flatMap { case (operationTrace, started, failed) =>
            onOperationStartedListeners.foreach(_ (deploymentRequest, started, failed))
            (if (started == 0) closeOperation(operationTrace, deploymentRequest) else Future.successful()).map(_ =>
              operationTrace
            )
          }
          .recover {
            case e: Throwable =>
              dbBinding.releaseLocks(deploymentRequest.id)
              throw e
          }
    }

  private def startDeploymentRequest(req: DeepDeploymentRequest, initiatorName: String, atCreation: Boolean): Future[ShallowOperationTrace] = {
    startOperation(
      req,
      operationStarter.start(targetResolver, targetDispatcher, req, Operation.deploy, initiatorName),
      listeners.map(listener => listener.onDeploymentRequestStarted(_: DeepDeploymentRequest, _: Int, _: Int, atCreation))
    )
  }

  private def closeOperation(operationTrace: ShallowOperationTrace, deploymentRequest: DeepDeploymentRequest): Future[ShallowOperationTrace] =
    dbBinding.closeOperationTrace(operationTrace)
      .map(_.map((_, true)).getOrElse((operationTrace, false)))
      .flatMap { case (trace, updated) =>
        dbBinding.findOperationEffect(trace).flatMap(_
          .map { effect =>
            val (_, status) = computeState(effect)
            dbBinding.releaseLocks(deploymentRequest.id).map { _ =>
              if (updated)
                listeners.foreach(
                  if (status == OperationStatus.succeeded)
                    _.onOperationSucceeded(trace, deploymentRequest)
                  else
                    _.onOperationFailed(trace, deploymentRequest)
                )
              trace
            }
          }
          .getOrElse(
            Future.successful(trace)
          )
        )
      }

  def isDeploymentRequestStarted(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Boolean)]] =
    dbBinding.isDeploymentRequestStarted(deploymentRequestId)

  private def rejectIfOutdated(deploymentRequest: DeploymentRequest): Future[Unit] =
    dbBinding.isOutdated(deploymentRequest).flatMap(
      if (_)
        Future.failed(Conflict("a newer one has already been applied"))
      else
        Future.successful()
    )

  def canDeployDeploymentRequest(deploymentRequest: DeploymentRequest): Future[Unit] =
    rejectIfOutdated(deploymentRequest)

  def canRevertDeploymentRequest(deploymentRequest: DeploymentRequest, isStarted: Boolean): Future[Unit] =
    if (!isStarted)
      Future.failed(UnavailableAction("it has not yet been applied"))
    else {
      // todo: now we can allow successive rollbacks,
      // by using dbBinding.findTargetAtomNotActionableBy instead of `outdated` here
      rejectIfOutdated(deploymentRequest)
    }


  def startDeploymentRequest(deploymentRequestId: Long, initiatorName: String): Future[Option[ShallowOperationTrace]] = {
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId).flatMap(
      _.map { req =>
        val check = if (withTransactions)
          dbBinding.findLastOperationAndEffect(req.productId).map { lastEffect =>
            lastEffect.foreach { operationEffect =>
              val requestId = operationEffect.operationTrace.deploymentRequestId
              val error = computeState(operationEffect) match {
                case (_, OperationStatus.inProgress) => // todo: as long as we don't have locking mechanism only
                  Some(s"wait for deployment request #$requestId to end")
                case (Operation.deploy, OperationStatus.failed) =>
                  Some(s"deployment request #$requestId has been left in an uncertain state, complete it first")
                case _ => None
              }
              error.foreach { message => throw Conflict(message, Seq(requestId)) }
            }
          }
        else
          Future.successful()

        check.flatMap(_ =>
          startDeploymentRequest(req, initiatorName, atCreation = false).map(Some(_))
        )
      }.getOrElse(Future.successful(None))
    )
  }

  def deployAgain(deploymentRequestId: Long, initiatorName: String): Future[Option[ShallowOperationTrace]] = {
    dbBinding.findDeepDeploymentRequestAndSpecs(deploymentRequestId).flatMap(
      _.map { case (deploymentRequest, executionSpecs) =>
        startOperation(
          deploymentRequest,
          operationStarter.deployAgain(targetResolver, targetDispatcher, deploymentRequest, executionSpecs, initiatorName),
          listeners.map(listener => listener.onDeploymentRequestRetried(_, _, _))
        ).map(Some(_))
      }.getOrElse(Future.successful(None))
    )
  }

  def findExecutionSpecificationsForRevert(deploymentRequest: DeploymentRequest): Future[(Select, Iterable[(ExecutionSpecification, Select)])] =
    dbBinding.findExecutionSpecificationsForRevert(deploymentRequest)

  def revert(deploymentRequestId: Long, initiatorName: String, defaultVersion: Option[Version]): Future[Option[ShallowOperationTrace]] =
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId).flatMap(
      _.map { depReq =>
        startOperation(
          depReq,
          operationStarter.revert(targetDispatcher, depReq, initiatorName, defaultVersion),
          listeners.map(listener => listener.onDeploymentRequestReverted(_, _, _))
        ).map(Some(_))
      }.getOrElse(Future.successful(None))
    )

  def findOperationTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowOperationTrace]]] =
    dbBinding.findOperationTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowExecutionTrace]]] =
    dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def updateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, logHref: String, statusMap: Map[String, TargetAtomStatus]): Future[Option[Unit]] = {
    val executionUpdate =
      if (logHref.nonEmpty)
        dbBinding.updateExecutionTrace(id, executionState, detail, logHref)
      else
        dbBinding.updateExecutionTrace(id, executionState, detail)

    executionUpdate.flatMap {
      if (_) {
        // the execution trace has been updated, so it must exist!
        dbBinding.findExecutionTraceById(id).map(_.get).flatMap { execTrace =>
          val op = execTrace.operationTrace

          dbBinding.updateTargetStatuses(execTrace.executionId, statusMap)
            .flatMap(_ => dbBinding.hasOpenExecutionTracesForOperation(op.id))
            .flatMap { hasOpenExecutions =>
              if (hasOpenExecutions)
                Future.successful(Some())
              else
                dbBinding.findDeepDeploymentRequestById(op.deploymentRequestId).flatMap { depReq =>
                  closeOperation(op, depReq.get).map(_ => Some())
                }
            }
        }
      }
      else
        Future.successful(None)
    }
  }

  def findDeepDeploymentRequestAndEffects(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Iterable[OperationEffect])]] =
    dbBinding.findDeepDeploymentRequestAndEffects(deploymentRequestId)

  def queryDeepDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[(DeepDeploymentRequest, Option[OperationEffect])]] =
    dbBinding.deepQueryDeploymentRequests(where, orderBy, limit, offset)

  def computeState(operationEffect: OperationEffect): (Operation.Kind, OperationStatus.Value) = {
    val OperationEffect(operationTrace, executionTraces, targetStatuses) = operationEffect
    val lastOperationState = operationTrace.closingDate.map { _ =>
      if (targetStatuses.forall(_.code == Status.notDone))
        OperationStatus.flopped
      else if (targetStatuses.forall(_.code == Status.success) && executionTraces.forall(_.state == ExecutionState.completed))
        OperationStatus.succeeded
      else
        OperationStatus.failed
    }.getOrElse(
      OperationStatus.inProgress
    )
    (operationTrace.kind, lastOperationState)
  }

}
