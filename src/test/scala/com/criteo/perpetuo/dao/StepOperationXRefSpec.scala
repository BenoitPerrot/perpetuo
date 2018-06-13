package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.ProtoDeploymentPlanStep
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
    with TestHelpers {

  test("Deployment plan steps can be inserted and retrieved") {
    val op1 = deploy("humanity", "v1", Seq("af"))
    val depReq = op1.deploymentRequest
    await(
      for {
        step2 <- insertDeploymentPlanStep(depReq, ProtoDeploymentPlanStep("Eurasia", JsArray(JsString("eu"), JsString("as")), ""))
        _ <- insertDeploymentPlanStep(depReq, ProtoDeploymentPlanStep("America", JsArray(JsString("am")), ""))
        xrefsStep2NotStarted <- findStepOperationXRefs(step2)
        op2 <- crankshaft.step(depReq, Some(1), "initiator", emitEvent = false)
        xrefsOp1 <- findStepOperationXRefs(op1)
        xrefsOp2 <- findStepOperationXRefs(op2)
        // todo: add tests when a specific plan step will be picked by a deploy
      } yield {
        xrefsStep2NotStarted.size shouldBe 0
        xrefsOp1.map(x => x.deploymentPlanStepId) shouldEqual Seq(step2.id - 1) // op1 has been applied on the first step only
        xrefsOp2.map(x => x.deploymentPlanStepId) shouldEqual Seq(step2.id) // op2 has been applied on second steps only
        xrefsOp1.map(x => x.operationTraceId).toSet shouldEqual Set(op1.id)
        xrefsOp2.map(x => x.operationTraceId).toSet shouldEqual Set(op2.id)
      }
    )
  }
}
