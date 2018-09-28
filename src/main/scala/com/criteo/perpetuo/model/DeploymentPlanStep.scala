package com.criteo.perpetuo.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import spray.json.JsValue

// TODO: rename to ParsedTarget once removed from DeploymentRequest (DREDD-982)
trait ParsedTarget {
  val targetExpression: JsValue

  def parsedTarget: TargetExpr = // TODO: once the target is removed from DeploymentRequest (DREDD-982), check whether a cache is needed here too
    DeploymentRequestParser.parseRootTargetExpression(targetExpression)
}

case class ProtoDeploymentPlanStep(name: String,
                                   targetExpression: JsValue,
                                   comment: String) extends ParsedTarget

@JsonIgnoreProperties(Array("deploymentRequest"))
case class DeploymentPlanStep(id: Long,
                              deploymentRequest: DeploymentRequest,
                              name: String,
                              targetExpression: JsValue,
                              comment: String) extends ParsedTarget

case class DeploymentPlan(deploymentRequest: DeploymentRequest,
                          steps: Iterable[DeploymentPlanStep]) {
  assert(steps.forall(_.deploymentRequest.id == deploymentRequest.id))
}
