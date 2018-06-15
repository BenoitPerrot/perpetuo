package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeploymentRequest, OperationTrace, ProtoDeploymentRequest}

import scala.concurrent.Future


trait SyncListener {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: ProtoDeploymentRequest): Unit

  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest): Unit

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestRetried(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestReverted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestStopped(deploymentRequest: DeploymentRequest, stopped: Int, failed: Int): Unit

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest): Unit

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest): Unit
}

trait AsyncListener {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: ProtoDeploymentRequest): Future[Unit]

  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest): Future[Unit]

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestRetried(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestReverted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestStopped(deploymentRequest: DeploymentRequest, stopped: Int, failed: Int): Future[Unit]

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest): Future[Unit]

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest): Future[Unit]
}
