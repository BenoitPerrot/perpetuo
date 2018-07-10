package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeploymentPlanStep, DeploymentRequest, Operation, OperationEffect}

case class DeploymentRequestStatus(deploymentRequest: DeploymentRequest,
                                   deploymentPlanSteps: Seq[DeploymentPlanStep],
                                   operationEffects: Iterable[OperationEffect],
                                   lastOperationStatus: Option[(Operation.Kind, DeploymentStatus.Value)], // fixme
                                   eligibleActions: Seq[(Operation.Kind, Option[String])],
                                   canAccessLogs: Boolean)
