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
  def requestDeployment(user: User, protoDeploymentRequest: ProtoDeploymentRequest, targets: Set[String]): Future[DeploymentRequest] = {
    if (!permissions.isAuthorized(user, DeploymentAction.requestOperation, Operation.deploy, protoDeploymentRequest.productName, targets))
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

  def step(user: User, deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[DeepOperationTrace]] =
    withDeepDeploymentRequest(deploymentRequestId) { (deploymentRequest, _) =>
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
        throw PermissionDenied()

      crankshaft
        .canDeployDeploymentRequest(deploymentRequest)
        .flatMap(_ => crankshaft.step(deploymentRequest, operationCount, user.name))
    }

  def deviseRevertPlan(id: Long): Future[Option[(Select, Iterable[(ExecutionSpecification, Select)])]] =
    withDeepDeploymentRequest(id) { (deploymentRequest, isStarted) =>
      crankshaft
        .canRevertDeploymentRequest(deploymentRequest, isStarted)
        .recover { case e: RejectingError => throw e.copy(s"Cannot revert the request #${deploymentRequest.id}: ${e.msg}") } // TODO: remove the copy
        .flatMap(_ => crankshaft.findExecutionSpecificationsForRevert(deploymentRequest))
    }

  def revert(user: User, deploymentRequestId: Long, operationCount: Option[Int], defaultVersion: Option[Version]): Future[Option[DeepOperationTrace]] =
    withDeepDeploymentRequest(deploymentRequestId) { (deploymentRequest, isStarted) =>
      if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
        throw PermissionDenied()

      crankshaft
        .canRevertDeploymentRequest(deploymentRequest, isStarted)
        .flatMap(_ => crankshaft.revert(deploymentRequest, user.name, defaultVersion))
    }

  // TODO: implement for multi-step
  def stop(user: User, id: Long, operationCount: Option[Int]): Future[Option[(Int, Seq[String])]] =
    crankshaft.findDeepDeploymentRequestById(id)
      .flatMap(_
        .map { deploymentRequest =>
          if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
            throw PermissionDenied()

          crankshaft
            .tryStopDeploymentRequest(deploymentRequest, user.name)
            .map(Some.apply)
        }
        .getOrElse(Future.successful(None))
      )

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentRequest, DeploymentStatus.Value, Option[Operation.Kind])]] =
    crankshaft.findDeploymentRequestsWithStatuses(where, limit, offset)

  def queryDeploymentRequestStatus(user: Option[User], id: Long): Future[Option[DeploymentRequestStatus]] =
    crankshaft
      .findDeepDeploymentRequestAndEffects(id)
      .flatMap(
        _.map { case (deploymentRequest, sortedEffects) =>
          val targets = deploymentRequest.parsedTarget.select

          val isAdmin = user.exists(user =>
            permissions.isAuthorized(user, GeneralAction.administrate)
          )

          // todo: a future workflow will differentiate requests and applies
          def authorized(op: Operation.Kind) = user.exists(user =>
            permissions.isAuthorized(user, DeploymentAction.applyOperation, op, deploymentRequest.product.name, targets)
          )

          Future
            .sequence(
              Operation.values.toSeq.map(action =>
                (action match {
                  case Operation.deploy => crankshaft.canDeployDeploymentRequest(deploymentRequest)
                  case Operation.revert => crankshaft.canRevertDeploymentRequest(deploymentRequest, sortedEffects.nonEmpty)
                })
                  .map(_ => Some((action, authorized(action))))
                  .recover { case _ => None }
              )
            )
            .map(authorizedActions =>
              Some(DeploymentRequestStatus(deploymentRequest, sortedEffects, sortedEffects.lastOption.map(crankshaft.computeState), authorizedActions.flatten, isAdmin || authorized(Operation.deploy)))
            )

        }.getOrElse(Future.successful(None))
      )

  def insertProductIfNotExists(user: User, name: String): Future[Product] = {
    if (!permissions.isAuthorized(user, GeneralAction.addProduct))
      throw PermissionDenied()

    crankshaft.insertProductIfNotExists(name)
  }
}
