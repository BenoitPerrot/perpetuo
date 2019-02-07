package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{ProtoDeploymentPlanStep, ProtoDeploymentRequest, Version}
import com.criteo.perpetuo.{SimpleScenarioTesting, TestHelpers}
import spray.json.{JsArray, JsString}

import scala.concurrent.ExecutionContext.Implicits.global

class StepOperationXRefSpec
  extends SimpleScenarioTesting
    with ProductBinder
    with DeploymentRequestBinder
    with OperationTraceBinder
    with DeploymentPlanStepBinder
    with StepOperationXRefBinder
    with DeploymentRequestInserter
    with TestHelpers {

  test("Deployment plan steps can be inserted and retrieved") {
    val productName = "humanity"
    val deploymentSteps = Seq(
      ProtoDeploymentPlanStep("Eurasia", JsArray(JsString("eu"), JsString("as")), ""),
      ProtoDeploymentPlanStep("America", JsArray(JsString("am")), "")
    )
    await(
      for {
        _ <- upsertProduct(productName)
        plan <- insertDeploymentRequest(ProtoDeploymentRequest(productName, Version("\"v42\""), deploymentSteps, "", "c.reator"))
        Seq(step1, step2) = plan.steps
        op1 <- step(plan.deploymentRequest, Some(0), "initiator")
        _ <- closeOperation(op1)
        xrefsStep2NotStarted <- findStepOperationXRefs(step2)
        op2 <- step(plan.deploymentRequest, Some(1), "initiator")
        xrefsOp1 <- findStepOperationXRefs(op1)
        xrefsOp2 <- findStepOperationXRefs(op2)
      } yield {
        xrefsStep2NotStarted.size shouldBe 0
        xrefsOp1.map(x => x.deploymentPlanStepId) shouldEqual Seq(step1.id) // op1 has been applied on the first step only
        xrefsOp2.map(x => x.deploymentPlanStepId) shouldEqual Seq(step2.id) // op2 has been applied on the second step only
        xrefsOp1.map(x => x.operationTraceId).toSet shouldEqual Set(op1.id)
        xrefsOp2.map(x => x.operationTraceId).toSet shouldEqual Set(op2.id)
      }
    )
  }
}
