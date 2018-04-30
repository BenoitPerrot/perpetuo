package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.{DeploymentAction, Permissions, User}
import com.criteo.perpetuo.model.{DeepOperationTrace, DeploymentRequestAttrs, Operation, Version}
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class PermissionDenied() extends RuntimeException

@Singleton
class Engine @Inject()(val crankshaft: Crankshaft,
                       val permissions: Permissions) {
  def requestDeployment(user: User, attrs: DeploymentRequestAttrs, targets: Set[String]): Future[Long] = {
    if (!permissions.isAuthorized(user, DeploymentAction.requestOperation, Operation.deploy, attrs.productName, targets))
      throw PermissionDenied()

    crankshaft.createDeploymentRequest(attrs)
  }

  def deploy(user: User, id: Long): Future[Option[DeepOperationTrace]] =
    crankshaft.isDeploymentRequestStarted(id)
      .flatMap(
        _.map { case (deploymentRequest, isStarted) =>
          if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
            throw PermissionDenied()

          crankshaft
            .canDeployDeploymentRequest(deploymentRequest)
            .flatMap(_ =>
              if (isStarted)
                crankshaft.deployAgain(id, user.name)
              else
                crankshaft.startDeploymentRequest(id, user.name)
            )
        }.getOrElse(Future.successful(None))
      )

  def revert(user: User, id: Long, defaultVersion: Option[Version]): Future[Option[DeepOperationTrace]] =
    crankshaft.isDeploymentRequestStarted(id)
      .flatMap(
        _.map { case (deploymentRequest, isStarted) =>
          if (!permissions.isAuthorized(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, deploymentRequest.parsedTarget.select))
            throw PermissionDenied()

          crankshaft
            .canRevertDeploymentRequest(deploymentRequest, isStarted)
            .flatMap(_ => crankshaft.revert(id, user.name, defaultVersion))
        }.getOrElse(Future.successful(None))
      )

  def stop(user: User, id: Long): Future[Option[(Int, Seq[String])]] =
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
}
