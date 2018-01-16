package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeepDeploymentRequest, OperationTrace}

trait Listener {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest, immediateStart: Boolean): String // fixme: should become Unit soon

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit
}
