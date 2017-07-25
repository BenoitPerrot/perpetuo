package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.Listener
import com.criteo.perpetuo.model.{DeploymentRequest, OperationTrace}


class DefaultListenerPlugin extends Listener with Plugin {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit = {}

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit = {}

  val timeout_s = 30
}


private[config] class ListenerPluginWrapper(implementation: Option[DefaultListenerPlugin]) extends PluginRunner(implementation, new DefaultListenerPlugin) with Listener {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String =
    wrap(_.onDeploymentRequestCreated(deploymentRequest, immediateStart))

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit =
    wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart, atCreation))

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit =
    wrap(_.onOperationClosed(operationTrace, deploymentRequest, succeeded))
}
