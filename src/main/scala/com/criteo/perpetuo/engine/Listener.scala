package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeepDeploymentRequest, OperationTrace}

trait Listener {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest, immediateStart: Boolean): String // fixme: should become Unit soon

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestRolledBack(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest, succeeded: Boolean): Unit
}
