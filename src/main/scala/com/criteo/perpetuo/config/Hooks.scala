package com.criteo.perpetuo.config

import com.criteo.perpetuo.model.DeploymentRequest

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


// fixme: temporarily parametrized
trait BaseHooks[T] {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): T

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit
}


class Hooks extends BaseHooks[String] with Plugin {
  /**
    * Methods that can be overridden as hooks.
    */
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit = {}
}


private[config] class HooksTrigger(implementation: Option[Hooks]) extends PluginRunner(implementation, new Hooks) with BaseHooks[Future[String]] {
  override def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): Future[String] =
    inFuture("onDeploymentRequestCreated", deploymentRequest, immediateStart)

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit =
    inFuture("onDeploymentRequestStarted", deploymentRequest, startedExecutions, failedToStart, immediately)

  protected def inFuture[T](methodName: String, args: Any*): Future[T] = {
    // start every hook method in a background thread
    Future {
      wrap[T](methodName, args: _*)
    }
  }
}
