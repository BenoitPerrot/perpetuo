package com.criteo.perpetuo.config

import com.criteo.perpetuo.model.{DeploymentRequest, OperationTrace}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


// fixme: temporarily parametrized
trait BaseHooks[T] {
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): T

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit
}


class Hooks extends BaseHooks[String] with Plugin {
  /**
    * Methods that can be overridden as hooks.
    */
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): String = null

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit = {}

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit = {}

  val timeout_s = 30
}


private[config] class HooksTrigger(implementation: Option[Hooks]) extends PluginRunner(implementation, new Hooks) with BaseHooks[Future[String]] {
  override def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest, immediateStart: Boolean): Future[String] =
    Future { wrap(_.onDeploymentRequestCreated(deploymentRequest, immediateStart), name = "onDeploymentRequestCreated") }

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int, atCreation: Boolean): Unit =
    Future { wrap(_.onDeploymentRequestStarted(deploymentRequest, startedExecutions, failedToStart, atCreation), name = "onDeploymentRequestStarted") }

  def onOperationClosed(operationTrace: OperationTrace, deploymentRequest: DeploymentRequest, succeeded: Boolean): Unit =
    Future { wrap(_.onOperationClosed(operationTrace, deploymentRequest, succeeded), name = "onOperationClosed") }
}
