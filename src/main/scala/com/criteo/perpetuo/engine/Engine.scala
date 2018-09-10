package com.criteo.perpetuo.engine

import java.util.concurrent.TimeUnit

import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, Permissions, User}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import javax.inject.{Inject, Singleton}
import slick.dbio.DBIOAction

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class PermissionDenied() extends RuntimeException("permission denied")

@Singleton
class Engine @Inject()(val crankshaft: Crankshaft,
                       val permissions: Permissions) {

  private def resolveTarget(productName: String, version: Version, target: Iterable[TargetExpr]) =
    target.foldLeft(Option(TargetAtomSet(Set()))) { (result, expr) =>
      result.flatMap(atoms =>
        crankshaft.targetResolver
          .resolveExpression(productName, version, expr)
          .map(_.union(atoms))
      )
    }

  def requestDeployment(user: User, protoDeploymentRequest: ProtoDeploymentRequest): Future[DeploymentRequest] = {
    val resolvedTarget = resolveTarget(protoDeploymentRequest.productName, protoDeploymentRequest.version, protoDeploymentRequest.plan.map(_.parsedTarget))
    if (!permissions.isAuthorized(user, DeploymentAction.requestOperation, Operation.deploy, protoDeploymentRequest.productName, resolvedTarget))
      Future.failed(PermissionDenied())
    else
      crankshaft.createDeploymentRequest(protoDeploymentRequest)
  }

  protected val stateCacheLoader: CacheLoader[java.lang.Long, Future[Option[DeploymentState]]] =
    new CacheLoader[java.lang.Long, Future[Option[DeploymentState]]]() {
      override def load(id: java.lang.Long): Future[Option[DeploymentState]] =
        withDeploymentRequest(id)(crankshaft.assessDeploymentState)
    }

  protected val cachedState: LoadingCache[java.lang.Long, Future[Option[DeploymentState]]] = CacheBuilder.newBuilder()
    .initialCapacity(128)
    .maximumSize(1024)
    .expireAfterWrite(2, TimeUnit.SECONDS)
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

  def step(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[OperationTrace]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      def resolveTargetAndRejectIfPermissionDenied(step: DeploymentPlanStep) = {
        val resolvedTarget = crankshaft.targetResolver.resolveExpression(deploymentRequest.product.name, deploymentRequest.version, step.parsedTarget)
        if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name, resolvedTarget))
          DBIOAction.failed(PermissionDenied())
        else
          DBIOAction.successful(resolvedTarget)
      }

      val getSpecifics =
        crankshaft.assessingDeploymentState(deploymentRequest)
          .flatMap {
            case _: Outdated =>
              DBIOAction.failed(Conflict("a newer one has already been applied", deploymentRequest.id))

            case _: Reverted | _: Deployed =>
              DBIOAction.failed(UnavailableAction("the deployment transaction is closed", Map("deploymentRequestId" -> deploymentRequest.id)))

            case _: RevertFailed =>
              DBIOAction.failed(UnavailableAction("the deployment transaction is being canceled", Map("deploymentRequestId" -> deploymentRequest.id)))

            case _: RevertInProgress | _: DeployInProgress =>
              DBIOAction.failed(UnavailableAction("another operation is already running", Map("deploymentRequestId" -> deploymentRequest.id)))

            case s: DeployFailed =>
              resolveTargetAndRejectIfPermissionDenied(s.step).flatMap(resolvedTarget => crankshaft.getRetrySpecifics(resolvedTarget, s.step).map((_, Some(s.step), s.step)))

            case s: DeployFlopped =>
              resolveTargetAndRejectIfPermissionDenied(s.step).flatMap(resolvedTarget => crankshaft.getRetrySpecifics(resolvedTarget, s.step).map((_, Some(s.step), s.step)))

            case s: NotStarted =>
              resolveTargetAndRejectIfPermissionDenied(s.toDo).flatMap(resolvedTarget => crankshaft.getStepSpecifics(resolvedTarget, s.toDo).map((_, None, s.toDo)))

            case s: Paused =>
              resolveTargetAndRejectIfPermissionDenied(s.toDo).flatMap(resolvedTarget => crankshaft.getStepSpecifics(resolvedTarget, s.toDo).map((_, Some(s.lastDone), s.toDo)))
          }

      crankshaft.step(deploymentRequest, operationCount, user.name, getSpecifics)
    }

  def deviseRevertPlan(id: Long): Future[Option[(TargetAtomSet, Iterable[(ExecutionSpecification, TargetAtomSet)])]] =
    withDeploymentRequest(id)(crankshaft.deviseRevertPlan)

  def revert(user: User, deploymentRequestId: Long, operationCount: Option[Int], defaultVersion: Option[Version]): Future[Option[OperationTrace]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      def rejectIfPermissionDenied(scope: Seq[DeploymentPlanStep]) = {
        val resolvedTarget = Engine.this.resolveTarget(deploymentRequest.product.name, deploymentRequest.version, scope.map(_.parsedTarget))
        if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, resolvedTarget))
          DBIOAction.failed(PermissionDenied())
        else
          DBIOAction.successful(())
      }

      val gettingRevertSpecifics =
        crankshaft.assessingDeploymentState(deploymentRequest)
          .flatMap {
            case _: Outdated =>
              DBIOAction.failed(Conflict("a newer one has already been applied", deploymentRequest.id))

            case _: Reverted =>
              DBIOAction.failed(UnavailableAction("the deployment transaction is closed", Map("deploymentRequestId" -> deploymentRequest.id)))

            case _: NotStarted | _: DeployFlopped =>
              DBIOAction.failed(UnavailableAction("Nothing to revert", Map("deploymentRequestId" -> deploymentRequest.id)))

            case _: RevertInProgress | _: DeployInProgress =>
              DBIOAction.failed(UnavailableAction("another operation is already running", Map("deploymentRequestId" -> deploymentRequest.id)))

            case s: RevertFailed =>
              rejectIfPermissionDenied(s.scope).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))

            case s: DeployFailed =>
              rejectIfPermissionDenied(s.revertScope).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))

            case s: Paused =>
              rejectIfPermissionDenied(s.revertScope).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))

            case s: Deployed =>
              rejectIfPermissionDenied(s.revertScope).flatMap(_ => crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ())))
          }

      crankshaft.revert(deploymentRequest, operationCount, user.name, gettingRevertSpecifics)
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
            operationCount.foreach(crankshaft.checkState(deploymentRequest, s.effects.length, _))
            val planStepsToStop = toPlanSteps(s, s.scope.deploymentPlanStepIds)
            val resolvedTarget = resolveTarget(deploymentRequest.product.name, deploymentRequest.version, planStepsToStop.map(_.parsedTarget))
            if (!permissions.isAuthorized(user, DeploymentAction.stopOperation, s.scope.operationTrace.kind, deploymentRequest.product.name, resolvedTarget))
              Future.failed(PermissionDenied())
            else
              crankshaft.tryStopOperation(s.scope, user.name).map(Some.apply)

          case _ =>
            Future.successful(Some(0, Seq()))
        }
    }
  }

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentPlan, DeploymentStatus.Value, Option[Operation.Kind])]] =
    crankshaft.findDeploymentRequestsWithStatuses(where, limit, offset)

  def findDeploymentRequestState(id: Long): Future[Option[DeploymentState]] =
    cachedState.get(id)

  def queryDeploymentRequestStatus(user: Option[User], id: Long): Future[Option[DeploymentRequestStatus]] =
    withDeploymentRequest(id) { deploymentRequest =>
      def rejectIfPermissionDenied(action: DeploymentAction.Value, kind: Operation.Kind, scope: Seq[DeploymentPlanStep]) = {
        val resolvedTarget = resolveTarget(deploymentRequest.product.name, deploymentRequest.version, scope.map(_.parsedTarget))
        if (!user.exists(permissions.isAuthorized(_, action, kind, deploymentRequest.product.name, resolvedTarget)))
          Some("permission denied")
        else
          None
      }

      crankshaft
        .assessDeploymentState(deploymentRequest)
        .map {
          case s: Outdated =>
            // TODO: no applicable actions
            (s, Seq(Operation.deploy.toString, Operation.revert.toString).map((_, Some("a newer one has already been applied"))))

          case s: Reverted =>
            (s, Seq())

          case s: Deployed =>
            (s, Seq((Operation.revert.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.revert, s.deploymentPlanSteps))))

          case s: NotStarted =>
            (s, Seq((Operation.deploy.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.deploy, Seq(s.toDo)))))

          case s: DeployFlopped =>
            (s, Seq((Operation.deploy.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.deploy, Seq(s.step)))))

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

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowExecutionTrace]]] =
    crankshaft.findExecutionTracesByDeploymentRequest(deploymentRequestId)

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
}
