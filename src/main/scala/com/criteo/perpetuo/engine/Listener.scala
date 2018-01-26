package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeepDeploymentRequest, OperationTrace}

import scala.concurrent.Future


trait SyncListener {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest): String

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Unit
}

trait AsyncListener {
  def onDeploymentRequestCreated(deploymentRequest: DeepDeploymentRequest): Future[String]

  def onDeploymentRequestStarted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestRetried(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onDeploymentRequestReverted(deploymentRequest: DeepDeploymentRequest, startedExecutions: Int, failedToStart: Int): Future[Unit]

  def onOperationFailed(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Future[Unit]

  def onOperationSucceeded(operationTrace: OperationTrace, deploymentRequest: DeepDeploymentRequest): Future[Unit]
}
