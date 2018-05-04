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
