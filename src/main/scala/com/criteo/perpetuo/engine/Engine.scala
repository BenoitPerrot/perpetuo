package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, Permissions, User}
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
    withDeploymentRequest(deploymentRequestId) { deploymentRequest =>
      def allowedTo(kind: Operation.Kind) =
        permissions.isAuthorized(user, DeploymentAction.applyOperation, kind, deploymentRequest.product.name)

      if (!(allowedTo(Operation.deploy) || allowedTo(Operation.revert)))
        throw PermissionDenied()

      crankshaft.tryStopDeploymentRequest(deploymentRequest, operationCount, user.name)
    }

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentRequest, DeploymentStatus.Value, Option[Operation.Kind])]] =
    crankshaft.findDeploymentRequestsWithStatuses(where, limit, offset)

  def queryDeploymentRequestStatus(user: Option[User], id: Long): Future[Option[DeploymentRequestStatus]] =
    crankshaft
      .findDeploymentRequestAndEffects(id)
      .flatMap(
        _.map { case (deploymentRequest, deploymentPlanSteps, effects) =>
          val isAdmin = user.exists(user =>
            permissions.isAuthorized(user, GeneralAction.administrate)
          )

          // todo: a future workflow will differentiate requests and applies
          def authorized(op: Operation.Kind) = user.exists(user =>
            permissions.isAuthorized(user, DeploymentAction.applyOperation, op, deploymentRequest.product.name)
          )

          crankshaft.getEligibleActions(deploymentRequest).map { actions =>
            val authorizedActions = actions.map { case (action, message) =>
              val isAuthorized = authorized(action)
              (action, isAuthorized && message.isEmpty, if (isAuthorized) message else Some("permission denied"))
            }
            val sortedEffects = effects.toSeq.sortBy(-_.operationTrace.id)
            Some(DeploymentRequestStatus(deploymentRequest, deploymentPlanSteps, sortedEffects, sortedEffects.headOption.map(crankshaft.computeState), authorizedActions, isAdmin || authorized(Operation.deploy)))
          }

        }.getOrElse(Future.successful(None))
      )

  def insertProductIfNotExists(user: User, name: String): Future[Product] = {
    if (!permissions.isAuthorized(user, GeneralAction.addProduct))
      throw PermissionDenied()

    crankshaft.insertProductIfNotExists(name)
  }
}
