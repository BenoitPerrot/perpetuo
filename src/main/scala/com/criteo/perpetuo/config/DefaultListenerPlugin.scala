package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.Listener
import com.criteo.perpetuo.model.{DeepDeploymentRequest, OperationTrace}


class DefaultListenerPlugin extends Listener with Plugin {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest, immediateStart: Boolean): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit = {}

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onDeploymentRequestRolledBack(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest, succeeded: Boolean): Unit = {}

  val timeout_s = 30
}


private[config] class ListenerPluginWrapper(implementation: DefaultListenerPlugin) extends PluginRunner(implementation, new DefaultListenerPlugin) with Listener {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest, immediateStart: Boolean): String =
    wrapTransition(_.onDeploymentRequestCreated(deploymentRequest, immediateStart))

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit =
    wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart, atCreation))

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit =
    wrap(_.onDeploymentRequestRetried(deploymentRequest, startedExecutions, failedToStart))

  def onDeploymentRequestRolledBack(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit =
    wrap(_.onDeploymentRequestRolledBack(deploymentRequest, startedExecutions, failedToStart))

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest, succeeded: Boolean): Unit =
    wrap(_.onOperationClosed(operationTrace, deploymentRequest, succeeded))
}
