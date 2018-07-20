package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.{AsyncListener, SyncListener}
import com.criteo.perpetuo.model._

import scala.concurrent.Future


class DefaultListenerPlugin extends SyncListener with Plugin {
  def onCreatingDeploymentRequest(protoDeploymentRequest: ProtoDeploymentRequest): Unit = {}

  def onDeploymentRequestCreated(deploymentPlan: DeploymentPlan): Unit = {}

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onDeploymentRequestRetried(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onDeploymentRequestReverted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit = {}

  def onDeploymentRequestStopped(deploymentRequest: DeploymentRequest, stopped: Int, failed: Int): Unit = {}

  def onOperationFailed(operationTrace: OperationTrace): Unit = {}

  def onOperationSucceeded(operationTrace: OperationTrace): Unit = {}

  def onTargetAtomStatusUpdate(operationTrace: OperationTrace, target: String, status: TargetAtomStatus): Unit = {}

  val timeout_s = 30
}


private[config] class ListenerPluginWrapper(implementation: DefaultListenerPlugin) extends PluginRunner(implementation, new DefaultListenerPlugin) with AsyncListener {
  def onCreatingDeploymentRequest(protoDeploymentRequest: ProtoDeploymentRequest): Future[Unit] =
    wrap(_.onCreatingDeploymentRequest(protoDeploymentRequest))

  def onDeploymentRequestCreated(deploymentPlan: DeploymentPlan): Future[Unit] =
    wrap(_.onDeploymentRequestCreated(deploymentPlan))

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit] =
    wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart))

  def onDeploymentRequestRetried(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit] =
    wrap(_.onDeploymentRequestRetried(deploymentRequest, startedExecutions, failedToStart))

  def onDeploymentRequestReverted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit] =
    wrap(_.onDeploymentRequestReverted(deploymentRequest, startedExecutions, failedToStart))

  def onDeploymentRequestStopped(deploymentRequest: DeploymentRequest, stopped: Int, failed: Int): Future[Unit] =
    wrap(_.onDeploymentRequestStopped(deploymentRequest, stopped, failed))

  def onOperationFailed(operationTrace: OperationTrace): Future[Unit] =
    wrap(_.onOperationFailed(operationTrace))

  def onOperationSucceeded(operationTrace: OperationTrace): Future[Unit] =
    wrap(_.onOperationSucceeded(operationTrace))

  def onTargetAtomStatusUpdate(operationTrace: OperationTrace, target: String, status: TargetAtomStatus): Future[Unit] =
    wrap(_.onTargetAtomStatusUpdate(operationTrace, target, status))
}
