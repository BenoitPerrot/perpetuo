package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine.TargetExpr
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import spray.json.JsValue

// TODO: rename to ParsedTarget once removed from DeploymentRequest (DREDD-982)
trait ParsedTarget_ {
  val targetExpression: JsValue

  def parsedTarget: TargetExpr = // TODO: once the target is removed from DeploymentRequest (DREDD-982), check whether a cache is needed here too
    DeploymentRequestParser.parseTargetExpression(targetExpression)
}

case class ProtoDeploymentPlanStep(name: String,
                                   targetExpression: JsValue,
                                   comment: String) extends ParsedTarget_

@JsonIgnoreProperties(Array("deploymentRequest"))
case class DeploymentPlanStep(id: Long,
                              deploymentRequest: DeploymentRequest,
                              name: String,
                              targetExpression: JsValue,
                              comment: String) extends ParsedTarget_

case class DeploymentPlan(deploymentRequest: DeploymentRequest,
                          steps: Iterable[DeploymentPlanStep]) {
  assert(steps.forall(_.deploymentRequest.id == deploymentRequest.id))
}
