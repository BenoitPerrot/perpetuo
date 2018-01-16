package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.Listener
import com.criteo.perpetuo.model.{DeepDeploymentRequest, OperationTrace}


class DefaultListenerPlugin extends Listener with Plugin {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest, immediateStart: Boolean): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit = {}

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit = {}

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit = {}

  val timeout_s = 30
}


private[config] class ListenerPluginWrapper(implementation: DefaultListenerPlugin) extends PluginRunner(implementation, new DefaultListenerPlugin) with Listener {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest, immediateStart: Boolean): String =
    wrapTransition(_.onDeploymentRequestCreated(deploymentRequest, immediateStart))

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit =
    wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart, atCreation))

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit =
    wrap(_.onDeploymentRequestRetried(deploymentRequest, startedExecutions, failedToStart))

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit =
    wrap(_.onDeploymentRequestReverted(deploymentRequest, startedExecutions, failedToStart))

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit =
    wrap(_.onOperationFailed(operationTrace, deploymentRequest))

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit =
    wrap(_.onOperationSucceeded(operationTrace, deploymentRequest))
}
