package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json.JsString

import scala.concurrent.ExecutionContext.Implicits.global


class ExecutionTraceSpec
  extends TestHelpers
    with ExecutionTraceBinder
    with ExecutionBinder
    with ExecutionSpecificationBinder
    with OperationTraceBinder
    with DeploymentRequestBinder
    with ProductBinder
    with DeploymentRequestInserter
    with TestDb {

  import dbContext.profile.api._

  test("ExecutionState values are all different") {
    ExecutionState.values
  }

  test("Execution traces can be bound to operation traces, and retrieved") {
    await(
      for {
        product <- insertProductIfNotExists("perpetuo-app")
        request <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "No fear", "c.norris")).map(_.deploymentRequest)
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "c.norris"))
        execSpec <- insertExecutionSpecification("{}", Version("\"456\""))
        execId <- dbContext.db.run(insertExecution(deployOperationTrace.id, execSpec.id))
        execTraceIds <- dbContext.db.run(insertingExecutionTraces(execId, 1))
        execTraces <- dbContext.db.run(executionTraceQuery.result)
      } yield {
        execTraces.length shouldEqual 1
        execTraces.head.id.get shouldEqual execTraceIds.head
        execTraces.head.executionId shouldEqual execId
        execTraces.head.logHref shouldBe empty
        execTraces.head.state shouldEqual ExecutionState.pending
      }
    )
  }
}
