package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model._

import scala.concurrent.Future


trait SyncListener {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: ProtoDeploymentRequest): Unit

  def onDeploymentRequestCreated(deploymentPlan: DeploymentPlan): Unit

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestResumed(deploymentPlanStep: DeploymentPlanStep, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestRetried(deploymentPlanStep: DeploymentPlanStep, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestReverted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestStopped(deploymentRequest: DeploymentRequest, stopped: Int, failed: Int): Unit

  def onDeploymentTransactionCanceled(deploymentRequest: DeploymentRequest): Unit

  def onDeploymentTransactionComplete(deploymentRequest: DeploymentRequest): Unit

  def onOperationFailed(operationTrace: OperationTrace): Unit

  def onOperationSucceeded(operationTrace: OperationTrace): Unit

  def onTargetAtomStatusUpdate(operationTrace: OperationTrace, target: String, status: TargetAtomStatus): Unit
}

trait AsyncListener {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: ProtoDeploymentRequest): Future[Unit]

  def onDeploymentRequestCreated(deploymentPlan: DeploymentPlan): Future[Unit]

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestResumed(deploymentPlanStep: DeploymentPlanStep, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestRetried(deploymentPlanStep: DeploymentPlanStep, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestReverted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestStopped(deploymentRequest: DeploymentRequest, stopped: Int, failed: Int): Future[Unit]

  def onDeploymentTransactionCanceled(deploymentRequest: DeploymentRequest): Future[Unit]

  def onDeploymentTransactionComplete(deploymentRequest: DeploymentRequest): Future[Unit]

  def onOperationFailed(operationTrace: OperationTrace): Future[Unit]

  def onOperationSucceeded(operationTrace: OperationTrace): Future[Unit]

  def onTargetAtomStatusUpdate(operationTrace: OperationTrace, target: String, status: TargetAtomStatus): Future[Unit]
}
