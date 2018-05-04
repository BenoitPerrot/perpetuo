package com.criteo.perpetuo.dao

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Version}
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

  test("Deployment requests can be inserted and retrieved") {
    Await.result(
      for {
        product <- insertProduct("humanity")
        deploymentRequest <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"v1\""), "", "", "f.sm"))
        step1 <- insertDeploymentPlanStep(deploymentRequest.id, "Africa", JsArray(JsString("af")), "")
        step2 <- insertDeploymentPlanStep(deploymentRequest.id, "Eurasia", JsArray(JsString("eu"), JsString("as")), "")
        step3 <- insertDeploymentPlanStep(deploymentRequest.id, "America", JsArray(JsString("am")), "")
        steps <- findDeploymentPlanStepsByRequestId(deploymentRequest.id)
      } yield {
        steps.size
      },
      1.second
    ) shouldBe 3
  }
}
