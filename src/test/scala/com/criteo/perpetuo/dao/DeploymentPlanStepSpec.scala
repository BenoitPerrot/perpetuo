package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model._
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

  import dbContext.profile.api._

  test("Deployment plan steps can be inserted and retrieved") {
    await(
      for {
        product <- upsertProduct("humanity")
        deploymentRequest <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v1\""), Seq(ProtoDeploymentPlanStep("Africa", JsArray(JsString("af")), "")), "", "f.sm")).map(_.deploymentRequest)
        _ <- insertDeploymentPlanStep(deploymentRequest, ProtoDeploymentPlanStep("Eurasia", JsArray(JsString("eu"), JsString("as")), ""))
        _ <- insertDeploymentPlanStep(deploymentRequest, ProtoDeploymentPlanStep("America", JsArray(JsString("am")), ""))
        nbSteps <- dbContext.db.run(deploymentPlanStepQuery.filter(_.deploymentRequestId === deploymentRequest.id).length.result)
      } yield {
        nbSteps shouldEqual 3
      }
    )
  }
}
