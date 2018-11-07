package com.criteo.perpetuo.metrics

import com.criteo.perpetuo.config.DefaultListenerPlugin
import com.criteo.perpetuo.model._
import com.samstarling.prometheusfinagle.metrics.Telemetry
import io.prometheus.client.{CollectorRegistry, Counter}

class DeploymentMetrics extends DefaultListenerPlugin {

  override def onCreatingDeploymentRequest(protoDeploymentRequest: ProtoDeploymentRequest): Unit =
    DeploymentMetrics.deployments.labels(protoDeploymentRequest.productName, "creating").inc()

  override def onDeploymentRequestCreated(deploymentPlan: DeploymentPlan): Unit =
    DeploymentMetrics.deployments.labels(deploymentPlan.deploymentRequest.product.name, "created").inc()

  override def onDeploymentRequestResumed(deploymentPlanStep: DeploymentPlanStep, startedExecutions: Int, failedToStart: Int): Unit =
    DeploymentMetrics.deployments.labels(deploymentPlanStep.deploymentRequest.product.name, "resumed").inc()

  override def onDeploymentTransactionCanceled(deploymentRequest: DeploymentRequest): Unit =
    DeploymentMetrics.deployments.labels(deploymentRequest.product.name, "canceled").inc()

  override def onDeploymentTransactionComplete(deploymentRequest: DeploymentRequest): Unit =
    DeploymentMetrics.deployments.labels(deploymentRequest.product.name, "completed").inc()

  override def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit =
    DeploymentMetrics.deployments.labels(deploymentRequest.product.name, "started").inc()

  override def onDeploymentRequestRetried(deploymentPlanStep: DeploymentPlanStep, startedExecutions: Int, failedToStart: Int): Unit =
    DeploymentMetrics.deployments.labels(deploymentPlanStep.deploymentRequest.product.name, "retried").inc()

  override def onDeploymentRequestReverted(deploymentRequest: DeploymentRequest, startedExecutions: Int, failedToStart: Int): Unit =
    DeploymentMetrics.deployments.labels(deploymentRequest.product.name, "reverted").inc()

  override def onDeploymentRequestStopped(deploymentRequest: DeploymentRequest, stopped: Int, failed: Int): Unit =
    DeploymentMetrics.deployments.labels(deploymentRequest.product.name, "stopped").inc()

  override def onOperationFailed(operationTrace: OperationTrace): Unit =
    DeploymentMetrics.operations.labels(operationTrace.deploymentRequest.product.name, "failed").inc()

  override def onOperationSucceeded(operationTrace: OperationTrace): Unit =
    DeploymentMetrics.operations.labels(operationTrace.deploymentRequest.product.name, "succeeded").inc()

  override def onTargetAtomStatusUpdate(operationTrace: OperationTrace, target: String, status: TargetAtomStatus): Unit =
    DeploymentMetrics.atomOperations.labels(operationTrace.deploymentRequest.product.name, status.code.toString()).inc()
}

private object DeploymentMetrics {
  val registry: CollectorRegistry = CollectorRegistry.defaultRegistry
  val telemetry: Telemetry = new Telemetry(registry, "deployment")

  val deployments: Counter = telemetry.counter(
    "deployments_total",
    "Number of deployments executed.",
    Seq("product", "state"))

  val operations: Counter = telemetry.counter(
    "operations_total",
    "Outcome of deployment operations.",
    Seq("product", "result"))

  val atomOperations: Counter = telemetry.counter(
    "atom_operations_total",
    "Outcome of operations executed on single targets.",
    Seq("product", "result"))
}

