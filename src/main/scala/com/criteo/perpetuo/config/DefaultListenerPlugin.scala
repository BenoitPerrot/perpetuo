package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.{AsyncListener, SyncListener}
import com.criteo.perpetuo.model.{DeepDeploymentRequest, DeploymentRequestAttrs, OperationTrace}

import scala.concurrent.Future


class DefaultListenerPlugin extends SyncListener with Plugin {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: DeploymentRequestAttrs): Unit = {}

  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest): Unit = {}

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit = {}

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit = {}

  val timeout_s = 30
}


private[config] class ListenerPluginWrapper(implementation: DefaultListenerPlugin) extends PluginRunner(implementation, new DefaultListenerPlugin) with AsyncListener {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: DeploymentRequestAttrs): Future[Unit] =
    wrapWithoutCatching(_.onCreatingDeploymentRequest(deploymentRequestAttrs))

  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest): Future[Unit] =
    wrap(_.onDeploymentRequestCreated(deploymentRequest))

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit] =
    wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart))

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit] =
    wrap(_.onDeploymentRequestRetried(deploymentRequest, startedExecutions, failedToStart))

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit] =
    wrap(_.onDeploymentRequestReverted(deploymentRequest, startedExecutions, failedToStart))

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Future[Unit] =
    wrap(_.onOperationFailed(operationTrace, deploymentRequest))

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Future[Unit] =
    wrap(_.onOperationSucceeded(operationTrace, deploymentRequest))
}
