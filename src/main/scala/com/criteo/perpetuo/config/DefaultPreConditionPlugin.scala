package com.criteo.perpetuo.config

import com.criteo.perpetuo.auth.{DeploymentAction, User}
import com.criteo.perpetuo.engine.{AsyncPreConditionEvaluator, PreConditionEvaluator}
import com.criteo.perpetuo.model.Operation.Kind
import com.criteo.perpetuo.model.{Operation, TargetAtom}

import scala.concurrent.Future
import scala.util.{Success, Try}


class DefaultPreConditionPlugin extends PreConditionEvaluator with Plugin {
  def canRequestDeployment(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit] = Success(())

  def canStep(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit] = Success(())

  def canRevert(user: Option[User], productName: String, targets: Set[TargetAtom]): Try[Unit] = Success(())

  def canQueryDeploymentRequestStatus(user: Option[User], action: DeploymentAction.Value, operation: Operation.Kind, productName: String, targets: Set[TargetAtom]): Try[Unit] = Success(())

  val timeout_s = 30
}

class AsyncPreConditionWrapper(implementation: DefaultPreConditionPlugin) extends PluginRunner(implementation, new DefaultPreConditionPlugin) with AsyncPreConditionEvaluator {
  override def canRequestDeployment(user: Option[User], productName: String, targets: Set[TargetAtom]): Future[Try[Unit]] =
    wrap(_.canRequestDeployment(user, productName, targets))

  override def canStep(user: Option[User], productName: String, targets: Set[TargetAtom]): Future[Try[Unit]] =
    wrap(_.canStep(user, productName, targets))

  override def canRevert(user: Option[User], productName: String, targets: Set[TargetAtom]): Future[Try[Unit]] =
    wrap(_.canRevert(user, productName, targets))

  override def canQueryDeploymentRequestStatus(user: Option[User], action: DeploymentAction.Value, operation: Kind,
    productName: String, targets: Set[TargetAtom]): Future[Try[Unit]] =
    wrap(_.canQueryDeploymentRequestStatus(user, action, operation, productName, targets))
}
