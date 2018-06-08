package com.criteo.perpetuo.dao

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.{ProtoDeploymentRequest, ProtoDeploymentPlanStep, Version}
import com.twitter.inject.Test
import org.junit.runner.RunWith
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import spray.json.{JsArray, JsString}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class DeploymentPlanStepSpec
  extends Test
    with ScalaFutures
    with ProductBinder
    with DeploymentRequestBinder
    with DeploymentPlanStepBinder
    with DeploymentRequestInserter
    with TestDb {

  test("Deployment plan steps can be inserted and retrieved") {
    Await.result(
      for {
        product <- insertProductIfNotExists("humanity")
        deploymentRequest <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v1\""), Seq(ProtoDeploymentPlanStep("Africa", JsArray(JsString("af")), "")), "", "f.sm")).map(_.deploymentRequest)
        _ <- insertDeploymentPlanStep(deploymentRequest.id, ProtoDeploymentPlanStep("Eurasia", JsArray(JsString("eu"), JsString("as")), ""))
        _ <- insertDeploymentPlanStep(deploymentRequest.id, ProtoDeploymentPlanStep("America", JsArray(JsString("am")), ""))
        plan <- findDeploymentPlan(deploymentRequest)
      } yield {
        plan.steps.size
      },
      1.second
    ) shouldBe 3
  }
}
