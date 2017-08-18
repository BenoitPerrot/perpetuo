package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeploymentRequest, OperationTrace}

trait Listener {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String // fixme: should become Unit soon

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit

  def onDeploymentRequestRetried(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestRolledBack(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit
}
