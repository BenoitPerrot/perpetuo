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
  def requestDeployment(user: User, protoDeploymentRequest: ProtoDeploymentRequest): Future[DeploymentRequest] = {
    if (!permissions.isAuthorized(user, DeploymentAction.requestOperation, Operation.deploy, protoDeploymentRequest.productName))
      throw PermissionDenied()

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
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name))
        throw PermissionDenied()

      def resolveTarget(step: DeploymentPlanStep) =
        crankshaft.targetResolver.resolveExpression(step.deploymentRequest.product.name, step.deploymentRequest.version, step.parsedTarget)

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
              crankshaft.getRetrySpecifics(resolveTarget(s.step), s.step).map((_, Some(s.step), s.step))

            case s: DeployFlopped =>
              crankshaft.getRetrySpecifics(resolveTarget(s.step), s.step).map((_, Some(s.step), s.step))

            case s: NotStarted =>
              crankshaft.getStepSpecifics(resolveTarget(s.toDo), s.toDo).map((_, None, s.toDo))

            case s: Paused =>
              crankshaft.getStepSpecifics(resolveTarget(s.toDo), s.toDo).map((_, Some(s.lastDone), s.toDo))
          }

      crankshaft.step(deploymentRequest, operationCount, user.name, getSpecifics)
    }

  def deviseRevertPlan(id: Long): Future[Option[(Set[TargetAtom], Iterable[(ExecutionSpecification, Set[TargetAtom])])]] =
    withDeploymentRequest(id)(crankshaft.deviseRevertPlan)

  def revert(user: User, deploymentRequestId: Long, operationCount: Option[Int], defaultVersion: Option[Version]): Future[Option[OperationTrace]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name))
        throw PermissionDenied()

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

            case _: RevertFailed | _: DeployFailed | _: Paused | _: Deployed =>
              crankshaft.getRevertSpecifics(deploymentRequest, defaultVersion).map((_, ()))
          }

      crankshaft.revert(deploymentRequest, operationCount, user.name, gettingRevertSpecifics)
    }

  def stop(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[(Int, Seq[String])]] = {
    flatWithDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      crankshaft
        .assessDeploymentState(deploymentRequest)
        .flatMap {
          case s: InProgressState =>
            operationCount.foreach(crankshaft.checkState(deploymentRequest, s.effects.length, _))
            if (!permissions.isAuthorized(user, DeploymentAction.stopOperation, s.scope.operationTrace.kind, deploymentRequest.product.name))
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
      def rejectIfPermissionDenied(action: DeploymentAction.Value, kind: Operation.Kind) =
        if (!user.exists(permissions.isAuthorized(_, action, kind, deploymentRequest.product.name)))
          Some("permission denied")
        else
          None

      crankshaft
        .assessDeploymentState(deploymentRequest)
        .map {
          case s: Outdated =>
            // TODO: no applicable actions
            (s, Seq(Operation.deploy.toString, Operation.revert.toString).map((_, Some("a newer one has already been applied"))))

          case s: Reverted =>
            (s, Seq())

          case s: Deployed =>
            (s, Seq((Operation.revert.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.revert))))

          case s@(_: NotStarted | _: DeployFlopped) =>
            (s, Seq((Operation.deploy.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.deploy))))

          case s: RevertInProgress =>
            (s, Seq(("stop", rejectIfPermissionDenied(DeploymentAction.stopOperation, Operation.revert))))

          case s: DeployInProgress =>
            (s, Seq(("stop", rejectIfPermissionDenied(DeploymentAction.stopOperation, Operation.deploy))))

          case s: RevertFailed =>
            (s, Seq((Operation.revert.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, Operation.revert))))

          case s@(_: DeployFailed | _: Paused) =>
            (s, Seq(Operation.deploy, Operation.revert).map(k => (k.toString, rejectIfPermissionDenied(DeploymentAction.applyOperation, k))))
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
