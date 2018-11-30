package com.criteo.perpetuo.auth

import com.criteo.perpetuo.config.DefaultPreConditionPlugin
import com.criteo.perpetuo.engine.{PermissionDenied, Unidentified}
import com.criteo.perpetuo.model.{DeploymentRequest, Operation, TargetAtom}

import scala.util.{Failure, Success, Try}

class AuthPreCondition(permissions: Permissions) extends DefaultPreConditionPlugin {

  private def evaluatePreconditions(user: Option[User], action: DeploymentAction.Value, operation: Operation.Kind, productName: String, targets: Set[TargetAtom]) =
    user
      .map(u =>
        if (permissions.isAuthorized(u, action, operation, productName, targets))
          Success(())
        else
          Failure(PermissionDenied())
      )
      .getOrElse(
        Failure(Unidentified())
      )

  override def canRequestDeployment(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit] =
    evaluatePreconditions(user, DeploymentAction.requestOperation, Operation.deploy, productName, targets)

  override def canStep(user: Option[User], deploymentRequest: DeploymentRequest, targets: Set[TargetAtom]): Try[Unit] =
    evaluatePreconditions(user, DeploymentAction.applyOperation, Operation.deploy, deploymentRequest.product.name, targets)

  override def canRevert(user: Option[User], deploymentRequest: DeploymentRequest, targets: Set[TargetAtom]): Try[Unit] =
    evaluatePreconditions(user, DeploymentAction.applyOperation, Operation.revert, deploymentRequest.product.name, targets)

  override def canQueryDeploymentRequestStatus(user: Option[User], deploymentRequest: DeploymentRequest, action: DeploymentAction.Value, operation: Operation.Kind, targets: Set[TargetAtom]): Try[Unit] =
    evaluatePreconditions(user, action, operation, deploymentRequest.product.name, targets)
}
