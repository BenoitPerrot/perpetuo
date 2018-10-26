package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.{DeploymentAction, User}
import com.criteo.perpetuo.model.{Operation, TargetAtom}

import scala.util.Try


trait PreConditionEvaluator {
  def canRequestDeployment(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit]

  def canStep(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit]

  def canRevert(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit]

  def canQueryDeploymentRequestStatus(user: Option[User], action: DeploymentAction.Value, operation: Operation.Kind, productName: String, targets: Set[TargetAtom]): Try[Unit]
}
