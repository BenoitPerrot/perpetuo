package com.criteo.perpetuo.config

import com.criteo.perpetuo.model.DeploymentRequest

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


// fixme: temporarily parametrized
trait BaseHooks[T] {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean, requestBody: String): T

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit
}


class Hooks extends BaseHooks[String] with Plugin {
  /**
    * Methods that can be overridden as hooks.
    */
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean, requestBody: String): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit = {}

  val timeout_s = 30
}


private[config] class HooksTrigger(implementation: Option[Hooks]) extends PluginRunner(implementation, new Hooks) with BaseHooks[Future[String]] {
  override def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean, requestBody: String): Future[String] =
    Future { wrap(_.onDeploymentRequestCreated(deploymentRequest, immediateStart, requestBody), name = "onDeploymentRequestCreated") }

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, immediately: Boolean): Unit =
    Future { wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart, immediately), name = "onDeploymentRequestStarted") }
}
