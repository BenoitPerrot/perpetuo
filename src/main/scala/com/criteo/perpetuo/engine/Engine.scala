package com.criteo.perpetuo.engine

import java.util.concurrent.TimeUnit

import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, Permissions, PerpetuoUser, User}
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.criteo.perpetuo.util.FutureLoadingCache
import com.google.common.base.Ticker
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.twitter.inject.Logging
import javax.inject.{Inject, Singleton}
import slick.dbio.DBIOAction

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

case class Unidentified() extends RuntimeException("unidentified")

case class PermissionDenied() extends RuntimeException("permission denied")

case class PreConditionFailed(errors: Seq[String]) extends RuntimeException((Seq("Some pre-conditions failed :") ++ errors).mkString("\n"))

@Singleton
class Engine @Inject()(val appConfig: AppConfig,
                       val crankshaft: Crankshaft,
                       val targetResolver: TargetResolver,
                       val permissions: Permissions,
                       val preConditionEvaluators: Seq[AsyncPreConditionEvaluator],
                       val scheduler: Scheduler) extends Logging {

  scheduler.scheduleTask(autoRevertFailingDeploymentRequests _, period = 5, timeUnit = TimeUnit.MINUTES)

  private def getTargetSuperset(productName: String, version: Version, target: Iterable[TargetExpr]): Set[TargetAtom] =
    target
      .map(targetResolver.resolveExpression(productName, version, _).superset)
      .reduceOption(_ union _)
      .getOrElse(Set.empty)

  private def evaluatePreconditions(canDo: AsyncPreConditionEvaluator => Future[Try[Unit]]): Future[Unit] = {
    val preConditions = preConditionEvaluators.map(canDo)
    Future.sequence(preConditions)
      .map(_.collect { case Failure(f) => f })
      .flatMap(failures =>
        failures.headOption
          .map(_ =>
            Future.failed(
              failures.collectFirst {
                case f@(_: PermissionDenied | _:Unidentified) => f
              }
              .getOrElse(
                PreConditionFailed(failures.map(_.getMessage))
              )
            )
          )
        .getOrElse(Future.successful(()))
      )
  }

  def requestDeployment(user: User, protoDeploymentRequest: ProtoDeploymentRequest): Future[DeploymentRequest] = {
    val targetSuperset = getTargetSuperset(protoDeploymentRequest.productName, protoDeploymentRequest.version, protoDeploymentRequest.plan.map(_.parsedTarget))
    evaluatePreconditions(_.canRequestDeployment(Some(user), protoDeploymentRequest.productName, targetSuperset))
      .flatMap(_ => crankshaft.createDeploymentRequest(protoDeploymentRequest))
  }

  protected val stateExpirationTime: FiniteDuration = Duration(
    appConfig.config.getLong("engine.cache.stateExpirationTimeInMs"),
    MILLISECONDS
  )
  protected val ticker: Ticker = Ticker.systemTicker()
  protected lazy val stateCache: FutureLoadingCache[java.lang.Long, Option[DeploymentState]] = {
    val stateCacheLoader: CacheLoader[java.lang.Long, Future[Option[DeploymentState]]] =
      new CacheLoader[java.lang.Long, Future[Option[DeploymentState]]]() {
        override def load(id: java.lang.Long): Future[Option[DeploymentState]] =
          withDeploymentRequest(id)(crankshaft.assessDeploymentState)
      }
    new FutureLoadingCache(
      CacheBuilder.newBuilder()
        .initialCapacity(128)
        .maximumSize(1024)
        .expireAfterWrite(stateExpirationTime.toNanos, NANOSECONDS)
        .concurrencyLevel(10)
        .ticker(ticker)
        .build(stateCacheLoader)
    )
  }

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
          case Success(_) => DBIOAction.from({
            val resolvedTarget = targetResolver.resolveExpression(deploymentRequest.product.name, deploymentRequest.version, step.parsedTarget)
            evaluatePreconditions(_.canStep(Some(user), deploymentRequest, resolvedTarget.superset))
              .map { _ => resolvedTarget }
          })
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
            case s if s.isOutdated =>
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
      def evaluatePreconditions(scope: Seq[DeploymentPlanStep], effects: Seq[OperationEffect]) =
        failOnUnexpectedOperationCount(operationCount, effects) match {
          case Failure(t) => DBIOAction.failed(t)
          case Success(_) => DBIOAction.from({
            val targetSuperset = getTargetSuperset(deploymentRequest.product.name, deploymentRequest.version, scope.map(_.parsedTarget))
            Engine.this.evaluatePreconditions(_.canRevert(Some(user), deploymentRequest, targetSuperset))
          })
        }

      val gettingRevertSpecifics =
        crankshaft.assessingDeploymentState(deploymentRequest)
          .map {
            case s@(_: DeployFailed | _: Paused) if s.isOutdated && crankshaft.fuelFilter.withTransactions =>
              // fixme: find another way to unblock situations where it's outdated yet holding transaction locks
              Right(s.asInstanceOf[RevertibleState])

            case s if s.isOutdated =>
              Left(DeploymentRequestOutdated(s.effects))

            case s: Abandoned =>
              Left(DeploymentRequestAbandoned(s.effects))

            case s: Reverted =>
              Left(DeploymentTransactionClosed(s.effects))

            case s@(_: NotStarted | _: DeployFlopped) =>
              Left(NothingToRevert(s.effects))

            case s@(_: RevertInProgress | _: DeployInProgress) =>
              Left(OperationRunning(s.effects))

            case s: RevertibleState =>
              Right(s)
          }
          .flatMap(_.fold(DBIOAction.failed, s => evaluatePreconditions(s.revertScope, s.effects)))
          .andThen(crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))


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

  def findDeploymentRequestsStates(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[DeploymentState]] =
    crankshaft
      .findDeploymentRequests(where, limit, offset)
      .flatMap(deploymentRequests =>
        Future.sequence(
          deploymentRequests.map(deploymentRequest => findDeploymentRequestState(deploymentRequest.id))
        ).map(_.flatten)
      )

  def findDeploymentRequestState(id: Long): Future[Option[DeploymentState]] =
    stateCache.get(id)

  def queryDeploymentRequestStatus(user: Option[User], id: Long): Future[Option[(DeploymentState, Seq[(String, Option[String])])]] =
    withDeploymentRequest(id) { deploymentRequest =>
      def evaluatePreconditions(action: DeploymentAction.Value, kind: Operation.Kind, scope: Seq[DeploymentPlanStep]) = {
        val targetSuperset = getTargetSuperset(deploymentRequest.product.name, deploymentRequest.version, scope.map(_.parsedTarget))
        Engine.this.evaluatePreconditions(_.canQueryDeploymentRequestStatus(user, deploymentRequest, action, kind, targetSuperset))
      }

      // TODO: rework: let caller decide how to serialize
      crankshaft
        .assessDeploymentState(deploymentRequest)
        .map {
          case s@(_: DeployFailed | _: Paused) if s.isOutdated && crankshaft.fuelFilter.withTransactions =>
            // fixme: find another way to unblock situations where it's outdated yet holding transaction locks
            (s, Seq((Operation.revert.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.revert, s.asInstanceOf[RevertibleState].revertScope))))

          case s if s.isOutdated =>
            (s, Seq())

          case s@ (_: Abandoned | _: Reverted) =>
            (s, Seq())

          case s: Deployed =>
            (s, Seq((Operation.revert.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.revert, s.deploymentPlanSteps))))

          case s: NotStarted =>
            (s, Seq(
              (Operation.deploy.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.deploy, Seq(s.toDo))),
              ("abandon", evaluatePreconditions(DeploymentAction.abandonOperation, Operation.deploy, s.deploymentPlanSteps))
            ))

          case s: DeployFlopped =>
            (s, Seq(
              (Operation.deploy.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.deploy, Seq(s.step))),
              ("abandon", evaluatePreconditions(DeploymentAction.abandonOperation, Operation.deploy, s.deploymentPlanSteps))
            ))

          case s: RevertInProgress =>
            (s, Seq(("stop", evaluatePreconditions(DeploymentAction.stopOperation, Operation.revert, toPlanSteps(s, s.scope.deploymentPlanStepIds)))))

          case s: DeployInProgress =>
            (s, Seq(("stop", evaluatePreconditions(DeploymentAction.stopOperation, Operation.deploy, toPlanSteps(s, s.scope.deploymentPlanStepIds)))))

          case s: RevertFailed =>
            (s, Seq((Operation.revert.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.revert, s.revertScope))))

          case s: DeployFailed =>
            (s, Seq(
              (Operation.deploy.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.deploy, Seq(s.step))),
              (Operation.revert.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.revert, s.revertScope))
            ))

          case s: Paused =>
            (s, Seq(
              (Operation.deploy.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.deploy, Seq(s.toDo))),
              (Operation.revert.toString, evaluatePreconditions(DeploymentAction.applyOperation, Operation.revert, s.revertScope))
            ))
        }
        .flatMap {
          case (state, actions) => Future.sequence(actions.map { case (s, f) => f.map(_ => (s, None)).recover {
            case e: Exception => (s, Some(e.getMessage))
          }}).map((state, _))
        }
    }

  def tryUpdateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, href: Option[String], statusMap: Map[TargetAtom, TargetAtomStatus] = Map()): Future[Option[(OperationTrace, Boolean)]] =
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

  def getCurrentVersionPerTarget(productName: String): Future[Map[TargetAtom, Version]] =
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget(productName)

  def autoRevertFailingDeploymentRequests: Future[Seq[Long]] =
    crankshaft.findAutoRevertibleDeploymentRequestIdsAndStateStamps
      .flatMap(depReqIdsAndStamps => Future.traverse(depReqIdsAndStamps) { case (depReqId, stateStamp) =>
        revert(PerpetuoUser, depReqId, Some(stateStamp), None)
          .map(_ => Some(depReqId))
          .recover {
            case _: MissingInfo | _: UnexpectedOperationCount =>
              None // If the revert failed for no default version or wrong operation count, silently ignore it
            case e: Exception => // Unexpected failure
              logger.error(s"Exception occurred when auto-reverting $depReqId: $e")
              None
          }
        }
        .map(_.flatten)
      )
}
