package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeepDeploymentRequest, Operation, OperationEffect}

case class DeploymentRequestStatus(deploymentRequest: DeepDeploymentRequest,
                                   operationEffects: Iterable[OperationEffect],
                                   lastOperationStatus: Option[(Operation.Kind, DeploymentStatus.Value)], // fixme
                                   authorizedActions: Seq[(Operation.Kind, Boolean)],
                                   canAccessLogs: Boolean)
