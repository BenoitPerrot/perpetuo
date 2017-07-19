package com.criteo.perpetuo.config

import com.criteo.perpetuo.model.{DeploymentRequest, OperationTrace}


trait BaseHooks {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String // fixme: should become Unit soon

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit
}


class Hooks extends BaseHooks with Plugin {
  /**
    * Methods that can be overridden as hooks.
    */
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit = {}

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit = {}

  val timeout_s = 30
}


private[config] class HooksTrigger(implementation: Option[Hooks]) extends PluginRunner(implementation, new Hooks) with BaseHooks {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String =
    wrap(_.onDeploymentRequestCreated(deploymentRequest, immediateStart))

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit =
    wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart, atCreation))

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit =
    wrap(_.onOperationClosed(operationTrace, deploymentRequest, succeeded))
}
