package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeepDeploymentRequest, ProtoDeploymentRequest, OperationTrace}

import scala.concurrent.Future


trait SyncListener {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: ProtoDeploymentRequest): Unit

  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest): Unit

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestStopped(deploymentRequest: DeepDeploymentRequest, stopped: Int, failed: Int): Unit

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit
}

trait AsyncListener {
  def onCreatingDeploymentRequest(deploymentRequestAttrs: ProtoDeploymentRequest): Future[Unit]

  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest): Future[Unit]

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestStopped(deploymentRequest: DeepDeploymentRequest, stopped: Int, failed: Int): Future[Unit]

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Future[Unit]

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Future[Unit]
}
