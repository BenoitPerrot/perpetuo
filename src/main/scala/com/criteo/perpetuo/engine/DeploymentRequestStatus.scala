package com.criteo.perpetuo.engine

import com.criteo.perpetuo.model.{DeploymentRequest, Operation, OperationEffect}

case class DeploymentRequestStatus(deploymentRequest: DeploymentRequest,
                                   operationEffects: Iterable[OperationEffect],
                                   lastOperationStatus: Option[(Operation.Kind, DeploymentStatus.Value)], // fixme
                                   authorizedActions: Seq[(Operation.Kind, Boolean)],
                                   canAccessLogs: Boolean)
