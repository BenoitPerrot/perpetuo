package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.{DeploymentAction, GeneralAction, Permissions, User}
import com.criteo.perpetuo.model._
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class PermissionDenied() extends RuntimeException

@Singleton
class Engine @Inject()(val crankshaft: Crankshaft,
                       val permissions: Permissions) {
  def requestDeployment(user: User, protoDeploymentRequest: ProtoDeploymentRequest): Future[DeploymentRequest] = {
    if (!permissions.isAuthorized(user, DeploymentAction.requestOperation, Operation.deploy, protoDeploymentRequest.productName))
      throw PermissionDenied()

    crankshaft.createDeploymentRequest(protoDeploymentRequest)
  }

  private def withDeepDeploymentRequest[T](id: Long)(callback: (DeploymentRequest, Boolean) => Future[T]): Future[Option[T]] =
    crankshaft.isDeploymentRequestStarted(id)
      .flatMap(
        _.map(callback.tupled)
          .map(_.map(Some.apply))
          .getOrElse(Future.successful(None))
      )

  def step(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[OperationTrace]] =
    withDeepDeploymentRequest(deploymentRequestId) { (deploymentRequest, _) =>
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name))
        throw PermissionDenied()

      crankshaft
        .rejectIfCannotDeploy(deploymentRequest)
        .flatMap(_ => crankshaft.step(deploymentRequest, operationCount, user.name))
    }

  def deviseRevertPlan(id: Long): Future[Option[(Select, Iterable[(ExecutionSpecification, Select)])]] =
    withDeepDeploymentRequest(id) { (deploymentRequest, isStarted) =>
      crankshaft.rejectIfLocked(deploymentRequest)
        .flatMap(_ => crankshaft.rejectIfCannotRevert(deploymentRequest, isStarted))
        .flatMap(_ => crankshaft.findExecutionSpecificationsForRevert(deploymentRequest))
    }

  def revert(user: User, deploymentRequestId: Long, operationCount: Option[Int], defaultVersion: Option[Version]): Future[Option[OperationTrace]] =
    withDeepDeploymentRequest(deploymentRequestId) { (deploymentRequest, isStarted) =>
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name))
        throw PermissionDenied()

      crankshaft
        .rejectIfCannotRevert(deploymentRequest, isStarted)
        .flatMap(_ => crankshaft.revert(deploymentRequest, operationCount, user.name, defaultVersion))
    }

  def stop(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[(Int, Seq[String])]] =
    withDeepDeploymentRequest(deploymentRequestId) { (deploymentRequest, _) =>
      def allowedTo(kind: Operation.Kind) =
        permissions.isAuthorized(user, DeploymentAction.applyOperation, kind, deploymentRequest.product.name)

      if (!(allowedTo(Operation.deploy) || allowedTo(Operation.revert)))
        throw PermissionDenied()

      crankshaft
        .tryStopDeploymentRequest(deploymentRequest, operationCount, user.name)
    }

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentRequest, DeploymentStatus.Value, Option[Operation.Kind])]] =
    crankshaft.findDeploymentRequestsWithStatuses(where, limit, offset)

  def queryDeploymentRequestStatus(user: Option[User], id: Long): Future[Option[DeploymentRequestStatus]] =
    crankshaft
      .findDeepDeploymentRequestAndEffects(id)
      .flatMap(
        _.map { case (deploymentRequest, deploymentPlanSteps, effects) =>
          val isAdmin = user.exists(user =>
            permissions.isAuthorized(user, GeneralAction.administrate)
          )

          // todo: a future workflow will differentiate requests and applies
          def authorized(op: Operation.Kind) = user.exists(user =>
            permissions.isAuthorized(user, DeploymentAction.applyOperation, op, deploymentRequest.product.name)
          )

          crankshaft.rejectIfLocked(deploymentRequest)
            .flatMap(_ =>
              Future
                .sequence(
                  Operation.values.toSeq.map { action =>
                    val canApply = action match {
                      case Operation.deploy => crankshaft.rejectIfCannotDeploy(deploymentRequest)
                      case Operation.revert => crankshaft.rejectIfCannotRevert(deploymentRequest, effects.nonEmpty)
                    }
                    canApply
                      .map(_ => Some((action, authorized(action))))
                      .recover { case _ => None }
                  }
                )
            )
            // todo: add the stop action in the Seq:
            .recover { case _ => Seq() }
            .map { authorizedActions =>
              val sortedEffects = effects.toSeq.sortBy(-_.operationTrace.id)
              Some(DeploymentRequestStatus(deploymentRequest, deploymentPlanSteps, sortedEffects, sortedEffects.headOption.map(crankshaft.computeState), authorizedActions.flatten, isAdmin || authorized(Operation.deploy)))
            }

        }.getOrElse(Future.successful(None))
      )

  def insertProductIfNotExists(user: User, name: String): Future[Product] = {
    if (!permissions.isAuthorized(user, GeneralAction.addProduct))
      throw PermissionDenied()

    crankshaft.insertProductIfNotExists(name)
  }
}
