package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.{DeploymentAction, User}
import com.criteo.perpetuo.model.{DeploymentRequest, Operation, TargetAtom}

import scala.concurrent.Future
import scala.util.Try


trait PreConditionEvaluator {
  def canRequestDeployment(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit]

  def canStep(user: Option[User], deploymentRequest: DeploymentRequest, targets: Set[TargetAtom]): Try[Unit]

  def canRevert(user: Option[User], deploymentRequest: DeploymentRequest, targets: Set[TargetAtom]): Try[Unit]

  def canQueryDeploymentRequestStatus(user: Option[User], deploymentRequest: DeploymentRequest, action: DeploymentAction.Value, operation: Operation.Kind, targets: Set[TargetAtom]): Try[Unit]
}

trait AsyncPreConditionEvaluator {
  def canRequestDeployment(user: Option[User], productName: String, targets: Set[TargetAtom]): Future[Try[Unit]]

  def canStep(user: Option[User], deploymentRequest: DeploymentRequest, targets: Set[TargetAtom]): Future[Try[Unit]]

  def canRevert(user: Option[User], deploymentRequest: DeploymentRequest, targets: Set[TargetAtom]): Future[Try[Unit]]

  def canQueryDeploymentRequestStatus(user: Option[User], deploymentRequest: DeploymentRequest, action: DeploymentAction.Value, operation: Operation.Kind, targets: Set[TargetAtom]): Future[Try[Unit]]
}
