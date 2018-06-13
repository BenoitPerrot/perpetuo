package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{ProtoDeploymentPlanStep, ProtoDeploymentRequest, Version}
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json.{JsArray, JsString}

import scala.concurrent.ExecutionContext.Implicits.global


class DeploymentPlanStepSpec
  extends TestHelpers
    with ProductBinder
    with DeploymentRequestBinder
    with DeploymentPlanStepBinder
    with DeploymentRequestInserter
    with TestDb {

  test("Deployment plan steps can be inserted and retrieved") {
    await(
      for {
        product <- insertProductIfNotExists("humanity")
        deploymentRequest <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v1\""), Seq(ProtoDeploymentPlanStep("Africa", JsArray(JsString("af")), "")), "", "f.sm")).map(_.deploymentRequest)
        _ <- insertDeploymentPlanStep(deploymentRequest, ProtoDeploymentPlanStep("Eurasia", JsArray(JsString("eu"), JsString("as")), ""))
        _ <- insertDeploymentPlanStep(deploymentRequest, ProtoDeploymentPlanStep("America", JsArray(JsString("am")), ""))
        plan <- findDeploymentPlan(deploymentRequest)
      } yield {
        plan.steps.size shouldEqual 3
      }
    )
  }
}
