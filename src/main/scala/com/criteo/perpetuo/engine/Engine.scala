package com.criteo.perpetuo.engine

import java.util.concurrent.TimeUnit

import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, Permissions, User}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import javax.inject.{Inject, Singleton}
import slick.dbio.DBIOAction

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

case class PermissionDenied() extends RuntimeException("permission denied")

@Singleton
class Engine @Inject()(val crankshaft: Crankshaft,
                       val permissions: Permissions) {

  private def getTargetSuperset(productName: String, version: Version, target: Iterable[TargetExpr]): Set[TargetAtom] =
    target
      .map(crankshaft.targetResolver.resolveExpression(productName, version, _).superset)
      .reduceOption(_ union _)
      .getOrElse(Set.empty)

  def requestDeployment(user: User, protoDeploymentRequest: ProtoDeploymentRequest): Future[DeploymentRequest] = {
    val targetSuperset = getTargetSuperset(protoDeploymentRequest.productName, protoDeploymentRequest.version, protoDeploymentRequest.plan.map(_.parsedTarget))
    if (!permissions.isAuthorized(user, DeploymentAction.requestOperation, Operation.deploy, protoDeploymentRequest.productName, targetSuperset))
      Future.failed(PermissionDenied())
    else
      crankshaft.createDeploymentRequest(protoDeploymentRequest)
  }

  protected val stateCacheLoader: CacheLoader[java.lang.Long, Future[Option[DeploymentState]]] =
    new CacheLoader[java.lang.Long, Future[Option[DeploymentState]]]() {
      override def load(id: java.lang.Long): Future[Option[DeploymentState]] =
        withDeploymentRequest(id)(crankshaft.assessDeploymentState)
    }

  private val stateCacheTimeExpiration = AppConfigProvider.config.getLong("engine.cache.stateExpirationTimeInMs")

  protected val cachedState: LoadingCache[java.lang.Long, Future[Option[DeploymentState]]] = CacheBuilder.newBuilder()
    .initialCapacity(128)
    .maximumSize(1024)
    .expireAfterWrite(stateCacheTimeExpiration, TimeUnit.MILLISECONDS)
    .concurrencyLevel(10)
    .build(stateCacheLoader)

  private def flatWithDeploymentRequest[T](id: Long)(callback: DeploymentRequest => Future[Option[T]]): Future[Option[T]] =
    crankshaft.findDeploymentRequestById(id)
      .flatMap(_
        .map(callback)
        .getOrElse(Future.successful(None))
      )

  private def withDeploymentRequest[T](id: Long)(callback: DeploymentRequest => Future[T]): Future[Option[T]] =
    flatWithDeploymentRequest(id)(deploymentRequest => callback(deploymentRequest).map(Some.apply))

  private def failOnUnexpectedOperationCount(operationCount: Option[Int], effects: Seq[OperationEffect]) =
    operationCount
      .filter(_ != effects.size)
      .map(_ => Failure(UnexpectedOperationCount(effects)))
      .getOrElse(Success(()))

  private def recoverOnSimilarOperation(user: User, deploymentRequest: DeploymentRequest, kind: Operation.Kind, operationCount: Option[Int]): PartialFunction[Throwable, Future[OperationTrace]] = {
    def isSimilar(operationTrace: OperationTrace) =
      operationTrace.kind == kind && operationTrace.creator == user.name

    {
      case e: OperationLockAlreadyTaken =>
        operationCount
          .map(c =>
            crankshaft
              .findOperationTraceForExpectedCount(deploymentRequest, c + 1)
              .map(_.filter(isSimilar))
          )
          .getOrElse(Future.successful(None))
          .flatMap(_
            .map(Future.successful)
            .getOrElse(Future.failed(e))
          )

      case e: OperationInapplicableForEffects =>
        operationCount
          .flatMap(e.effects.reverse.lift)
          .map(_.operationTrace)
          .filter(isSimilar)
          .map(Future.successful)
          .getOrElse(Future.failed(e))
    }
  }

  def step(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[OperationTrace]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      def evaluatePreconditionsForResolvedTarget(step: DeploymentPlanStep, effects: Seq[OperationEffect]) =
        failOnUnexpectedOperationCount(operationCount, effects) match {
          case Failure(t) => DBIOAction.failed(t)
          case Success(_) =>
            val resolvedTarget = crankshaft.targetResolver.resolveExpression(deploymentRequest.product.name, deploymentRequest.version, step.parsedTarget)
            if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name, resolvedTarget.superset))
              DBIOAction.failed(PermissionDenied())
            else
              DBIOAction.successful(resolvedTarget)
        }

      def getRetrySpecifics(step: DeploymentPlanStep, effects: Seq[OperationEffect]) =
        evaluatePreconditionsForResolvedTarget(step, effects).flatMap(targetSuperset =>
          crankshaft.getRetrySpecifics(targetSuperset, step, effects).map((_, Some(step), step))
        )

      def getStepSpecifics(toDo: DeploymentPlanStep, lastDone: Option[DeploymentPlanStep], lastEffects: Seq[OperationEffect]) =
        evaluatePreconditionsForResolvedTarget(toDo, lastEffects).flatMap(targetSuperset =>
          crankshaft.getStepSpecifics(targetSuperset, toDo).map((_, lastDone, toDo))
        )

      val getSpecifics =
        crankshaft.assessingDeploymentState(deploymentRequest)
          .flatMap {
            case s: Outdated =>
              DBIOAction.failed(DeploymentRequestOutdated(s.effects))

            case s: Abandoned =>
              DBIOAction.failed(DeploymentRequestAbandoned(s.effects))

            case s@(_: Reverted | _: Deployed | _: RevertFailed) =>
              DBIOAction.failed(DeploymentTransactionClosed(s.effects))

            case s@(_: RevertInProgress | _: DeployInProgress) =>
              DBIOAction.failed(OperationRunning(s.effects))

            case s: DeployFailed =>
              getRetrySpecifics(s.step, s.effects)

            case s: DeployFlopped =>
              getRetrySpecifics(s.step, s.effects)

            case s: NotStarted =>
              getStepSpecifics(s.toDo, None, s.effects)

            case s: Paused =>
              getStepSpecifics(s.toDo, Some(s.lastDone), s.effects)
          }

      crankshaft
        .step(deploymentRequest, user.name, getSpecifics)
        .recoverWith(recoverOnSimilarOperation(user, deploymentRequest, Operation.deploy, operationCount))
    }

  def deviseRevertPlan(id: Long): Future[Option[(Set[TargetAtom], Iterable[(ExecutionSpecification, Set[TargetAtom])])]] =
    withDeploymentRequest(id)(crankshaft.deviseRevertPlan)

  def revert(user: User, deploymentRequestId: Long, operationCount: Option[Int], defaultVersion: Option[Version]): Future[Option[OperationTrace]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      def rejectIfPermissionDenied(scope: Seq[DeploymentPlanStep], effects: Seq[OperationEffect]) =
        failOnUnexpectedOperationCount(operationCount, effects) match {
          case Failure(t) => DBIOAction.failed(t)
          case Success(_) =>
            val targetSuperset = getTargetSuperset(deploymentRequest.product.name, deploymentRequest.version, scope.map(_.parsedTarget))
            if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, targetSuperset))
              DBIOAction.failed(PermissionDenied())
            else
              DBIOAction.successful(())
        }

      val gettingRevertSpecifics =
        crankshaft.assessingDeploymentState(deploymentRequest)
          .flatMap {
            case s: Outdated =>
              DBIOAction.failed(DeploymentRequestOutdated(s.effects))

            case s: Abandoned =>
              DBIOAction.failed(DeploymentRequestAbandoned(s.effects))

            case s: Reverted =>
              DBIOAction.failed(DeploymentTransactionClosed(s.effects))

            case s@(_: NotStarted | _: DeployFlopped) =>
              DBIOAction.failed(NothingToRevert(s.effects))

            case s@(_: RevertInProgress | _: DeployInProgress) =>
              DBIOAction.failed(OperationRunning(s.effects))

            case s: RevertFailed =>
              rejectIfPermissionDenied(s.scope, s.effects).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))

            case s: DeployFailed =>
              rejectIfPermissionDenied(s.revertScope, s.effects).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))

            case s: Paused =>
              rejectIfPermissionDenied(s.revertScope, s.effects).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))

            case s: Deployed =>
              rejectIfPermissionDenied(s.revertScope, s.effects).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))
          }

      crankshaft
        .revert(deploymentRequest, operationCount, user.name, gettingRevertSpecifics)
        .recoverWith(recoverOnSimilarOperation(user, deploymentRequest, Operation.revert, operationCount))
    }

  // TODO: put deploymentPlanSteps in a TreeMap[id -> step] in state
  private def toPlanSteps(s: DeploymentState, planStepIds: Seq[Long]) = {
    val idToPlanStep = s.deploymentPlanSteps.map(planStep => planStep.id -> planStep).toMap
    planStepIds.flatMap(idToPlanStep.get)
  }

  def stop(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[(Int, Seq[String])]] = {
    flatWithDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      crankshaft
        .assessDeploymentState(deploymentRequest)
        .flatMap {
          case s: InProgressState =>
            failOnUnexpectedOperationCount(operationCount, s.effects) match {
              case Failure(t) => Future.failed(t)
              case Success(_) =>
                val planStepsToStop = toPlanSteps(s, s.scope.deploymentPlanStepIds)
                val targetSuperset = getTargetSuperset(deploymentRequest.product.name, deploymentRequest.version, planStepsToStop.map(_.parsedTarget))
                if (!permissions.isAuthorized(user, DeploymentAction.stopOperation, s.scope.operationTrace.kind, deploymentRequest.product.name, targetSuperset))
                  Future.failed(PermissionDenied())
                else
                  crankshaft.tryStopOperation(s.scope, user.name).map(Some.apply)
            }

          case _ =>
            Future.successful(Some(0, Seq()))
        }
    }
  }

  def abandon(user: User, deploymentRequestId: Long): Future[Option[Unit]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      crankshaft.assessDeploymentState(deploymentRequest).flatMap { state =>
        val targetSuperset = getTargetSuperset(deploymentRequest.product.name, deploymentRequest.version, state.deploymentPlanSteps.map(_.parsedTarget))
        if (permissions.isAuthorized(user, DeploymentAction.abandonOperation, Operation.deploy, deploymentRequest.product.name, targetSuperset))
          crankshaft.abandon(deploymentRequest)
        else
          Future.failed(PermissionDenied())
      }
    }

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentPlan, DeploymentStatus.Value, Option[Operation.Kind])]] =
    crankshaft.findDeploymentRequestsWithStatuses(where, limit, offset)

  def findDeploymentRequestState(id: Long): Future[Option[DeploymentState]] =
    cachedState.get(id)

  def queryDeploymentRequestStatus(user: Option[User], id: Long): Future[Option[DeploymentRequestStatus]] =
    withDeploymentRequest(id) { deploymentRequest =>
      def rejectIfPermissionDenied(action: DeploymentAction.Value, kind: Operation.Kind, scope: Seq[DeploymentPlanStep]) = {
        val targetSuperset = getTargetSuperset(deploymentRequest.product.name, deploymentRequest.version, scope.map(_.parsedTarget))
        if (!user.exists(permissions.isAuthorized(_, action, kind, deploymentRequest.product.name, targetSuperset)))
          Some("permission denied")
        else
          None
      }

      // TODO: rework: let caller decide how to serialize
      crankshaft
        .assessDeploymentState(deploymentRequest)
        .map {
          case s: Outdated =>
            // TODO: no applicable actions
            (s, Seq(Operation.deploy.toString, Operation.revert.toString).map((_, Some("a newer one has already been applied"))))

          case s: Abandoned =>
            (s, Seq())

          case s: Reverted =>
            (s, Seq())

          case s: Deployed =>
            (s, Seq((Operation.revert.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.revert, s.deploymentPlanSteps))))

          case s: NotStarted =>
            (s, Seq(
              (Operation.deploy.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.deploy, Seq(s.toDo))),
              ("abandon", rejectIfPermissionDenied(DeploymentAction.abandonOperation, Operation.deploy, s.deploymentPlanSteps))
            ))

          case s: DeployFlopped =>
            (s, Seq(
              (Operation.deploy.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.deploy, Seq(s.step))),
              ("abandon", rejectIfPermissionDenied(DeploymentAction.abandonOperation, Operation.deploy, s.deploymentPlanSteps))
            ))

          case s: RevertInProgress =>
            (s, Seq(("stop", rejectIfPermissionDenied(DeploymentAction.stopOperation, Operation.revert, toPlanSteps(s, s.scope.deploymentPlanStepIds)))))

          case s: DeployInProgress =>
            (s, Seq(("stop", rejectIfPermissionDenied(DeploymentAction.stopOperation, Operation.deploy, toPlanSteps(s, s.scope.deploymentPlanStepIds)))))

          case s: RevertFailed =>
            (s, Seq((Operation.revert.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.revert, s.scope))))

          case s: DeployFailed =>
            (s, Seq(
              (Operation.deploy.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.deploy, Seq(s.step))),
              (Operation.revert.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.revert, s.revertScope))
            ))

          case s: Paused =>
            (s, Seq(
              (Operation.deploy.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.deploy, Seq(s.toDo))),
              (Operation.revert.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.revert, s.revertScope))
            ))
        }
        .map { case (s, authorizedActions) =>
          DeploymentRequestStatus(s.deploymentRequest, s.deploymentPlanSteps, s.effects, s.effects.headOption.map(crankshaft.computeState), authorizedActions)
        }
    }

  def tryUpdateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, href: Option[String], statusMap: Map[TargetAtom, TargetAtomStatus] = Map()): Future[Option[Long]] =
    crankshaft.tryUpdateExecutionTrace(id, executionState, detail, href, statusMap)

  def getProducts: Future[Seq[Product]] =
    crankshaft.getProducts

  def upsertProduct(user: User, name: String, active: Boolean = true): Future[Product] = {
    if (!permissions.isAuthorized(user, GeneralAction.updateProduct))
      throw PermissionDenied()

    crankshaft.upsertProduct(name, active)
  }

  def setActiveProducts(user: User, names: Seq[String]): Future[Set[Product]] = {
    if (!permissions.isAuthorized(user, GeneralAction.updateProduct))
      throw PermissionDenied()

    crankshaft.setActiveProducts(names)
  }

  def allowedToUpdateProduct(user: User): Boolean =
    permissions.isAuthorized(user, GeneralAction.updateProduct)

  def getAllowedActions(user: User): Seq[GeneralAction.Value] =
    GeneralAction.values.filter(permissions.isAuthorized(user, _)).toSeq
}
