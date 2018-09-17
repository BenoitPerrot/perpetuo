package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, Permissions, User}
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import javax.inject.{Inject, Singleton}

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

  private def withDeploymentRequest[T](id: Long)(callback: DeploymentRequest => Future[T]): Future[Option[T]] =
    crankshaft.findDeploymentRequestById(id)
      .flatMap(
        _.map(callback)
          .map(_.map(Some.apply))
          .getOrElse(Future.successful(None))
      )

  def step(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[OperationTrace]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name))
        throw PermissionDenied()

      crankshaft.step(deploymentRequest, operationCount, user.name)
    }

  def deviseRevertPlan(id: Long): Future[Option[(Select, Iterable[(ExecutionSpecification, Select)])]] =
    withDeploymentRequest(id)(crankshaft.deviseRevertPlan)

  def revert(user: User, deploymentRequestId: Long, operationCount: Option[Int], defaultVersion: Option[Version]): Future[Option[OperationTrace]] =
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name))
        throw PermissionDenied()

      crankshaft.revert(deploymentRequest, operationCount, user.name, defaultVersion)
    }

  def stop(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[(Int, Seq[String])]] =
    crankshaft.findLastOperationTrace(deploymentRequestId, operationCount)
      .flatMap(_
        .map { operationTrace =>
          if (!permissions.isAuthorized(user, DeploymentAction.stopOperation, operationTrace.kind, operationTrace.deploymentRequest.product.name))
            throw PermissionDenied()

          crankshaft.tryStopOperation(operationTrace, user.name)
        }
        .map(_.map(Some.apply))
        .getOrElse(Future.successful(None))
      )

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentPlan, DeploymentStatus.Value, Option[Operation.Kind])]] =
    crankshaft.findDeploymentRequestsWithStatuses(where, limit, offset)

  def findDeploymentRequestState(id: Long): Future[Option[DeploymentState]] =
    withDeploymentRequest(id)(crankshaft.assessDeploymentState)

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

  def tryUpdateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, href: Option[String], statusMap: Map[String, TargetAtomStatus] = Map()): Future[Option[Long]] =
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
