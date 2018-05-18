package com.criteo.perpetuo.model

import spray.json.JsValue

case class ProtoDeploymentPlanStep(name: String,
                                   targetExpression: JsValue,
                                   comment: String)

case class DeploymentPlanStep(id: Long,
                              deploymentRequestId: Long,
                              name: String,
                              targetExpression: JsValue,
                              comment: String)


/**
  * It's not necessarily an entire deployment plan, it can be a subset of it,
  * but in any case a sequence of steps bound to a single deployment request.
  */
case class DeploymentPlan(deploymentRequest: DeepDeploymentRequest,
                          steps: Iterable[DeploymentPlanStep]) {
  assert(steps.forall(_.deploymentRequestId == deploymentRequest.id))
}
