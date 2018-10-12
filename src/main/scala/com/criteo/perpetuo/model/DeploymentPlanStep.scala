package com.criteo.perpetuo.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import spray.json.JsValue


trait ParsedTarget {
  val targetExpression: JsValue

  lazy val parsedTarget: TargetExpr =
    DeploymentRequestParser.parseRootTargetExpression(targetExpression)
}

case class ProtoDeploymentPlanStep(name: String,
                                   targetExpression: JsValue,
                                   comment: String) extends ParsedTarget

@JsonIgnoreProperties(Array("deploymentRequest", "parsedTarget"))
case class DeploymentPlanStep(id: Long,
                              deploymentRequest: DeploymentRequest,
                              name: String,
                              targetExpression: JsValue,
                              comment: String) extends ParsedTarget

case class DeploymentPlan(deploymentRequest: DeploymentRequest,
                          steps: Iterable[DeploymentPlanStep]) {
  assert(steps.forall(_.deploymentRequest.id == deploymentRequest.id))
}
