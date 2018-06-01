package com.criteo.perpetuo.dao

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.model.ProtoDeploymentPlanStep
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import spray.json.{JsArray, JsString}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class StepOperationXRefSpec
  extends SimpleScenarioTesting
    with ProductBinder
    with DeploymentRequestBinder
    with OperationTraceBinder
    with DeploymentPlanStepBinder
    with StepOperationXRefBinder {

  test("Deployment plan steps can be inserted and retrieved") {
    val op1 = deploy("humanity", "v1", Seq("af"))
    val depReq = op1.deploymentRequest
    val (op2, stepIds, stepIdsBoundToOp1, stepIdsBoundToOp2, opIdsBoundToOp1, opIdsBoundToOp2) = Await.result(
      for {
        step2 <- insertDeploymentPlanStep(depReq.id, ProtoDeploymentPlanStep("Eurasia", JsArray(JsString("eu"), JsString("as")), ""))
        _ <- insertDeploymentPlanStep(depReq.id, ProtoDeploymentPlanStep("America", JsArray(JsString("am")), ""))
        xrefsStep2NotStarted <- findStepOperationXRefs(step2)
        op2 <- crankshaft.startDeploymentRequest(depReq, "initiator", emitEvent = false)
        xrefsOp1 <- findStepOperationXRefs(op1)
        xrefsOp2 <- findStepOperationXRefs(op2)
        // todo: add tests when a specific plan step will be picked by a deploy
      } yield {
        xrefsStep2NotStarted.size shouldBe 0
        (
          op2,
          Range.inclusive(step2.id.toInt - 1, step2.id.toInt + 1),
          xrefsOp1.map(x => x.deploymentPlanStepId),
          xrefsOp2.map(x => x.deploymentPlanStepId),
          xrefsOp1.map(x => x.operationTraceId).toSet,
          xrefsOp2.map(x => x.operationTraceId).toSet
        )
      },
      1.second
    )

    stepIdsBoundToOp1 shouldEqual Seq(stepIds.start) // op1 has been applied on the first step only
    stepIdsBoundToOp2.sorted shouldEqual stepIds.toList // op2 has been applied on all steps (that will obviously be changed when there will be functions to appropriately execute the plan)
    opIdsBoundToOp1 shouldEqual Set(op1.id)
    opIdsBoundToOp2 shouldEqual Set(op2.id)
  }
}
