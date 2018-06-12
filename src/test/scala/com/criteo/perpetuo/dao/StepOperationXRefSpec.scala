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
    val (op2, step2, stepIdsBoundToOp1, stepIdsBoundToOp2, opIdsBoundToOp1, opIdsBoundToOp2) = Await.result(
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
        (
          op2,
          step2,
          xrefsOp1.map(x => x.deploymentPlanStepId),
          xrefsOp2.map(x => x.deploymentPlanStepId),
          xrefsOp1.map(x => x.operationTraceId).toSet,
          xrefsOp2.map(x => x.operationTraceId).toSet
        )
      },
      1.second
    )

    stepIdsBoundToOp1 shouldEqual Seq(step2.id - 1) // op1 has been applied on the first step only
    stepIdsBoundToOp2 shouldEqual Seq(step2.id) // op2 has been applied on second steps only
    opIdsBoundToOp1 shouldEqual Set(op1.id)
    opIdsBoundToOp2 shouldEqual Set(op2.id)
  }
}
