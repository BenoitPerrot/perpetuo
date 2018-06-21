package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine.TargetExpr
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import spray.json.JsValue

case class ProtoDeploymentPlanStep(name: String,
                                   targetExpression: JsValue,
                                   comment: String)

@JsonIgnoreProperties(Array("deploymentRequest"))
case class DeploymentPlanStep(id: Long,
                              deploymentRequest: DeploymentRequest,
                              name: String,
                              targetExpression: JsValue,
                              comment: String) {
  def parsedTarget: TargetExpr = // todo: once the target is removed from DeploymentRequest (DREDD-982), check whether a cache is needed here too
    DeploymentRequestParser.parseTargetExpression(targetExpression)
}


/**
  * It's not necessarily an entire deployment plan, it can be a subset of it,
  * but in any case a sequence of steps bound to a single deployment request.
  */
case class DeploymentPlan(deploymentRequest: DeploymentRequest,
                          steps: Iterable[DeploymentPlanStep]) {
  assert(steps.forall(_.deploymentRequest.id == deploymentRequest.id))
}
