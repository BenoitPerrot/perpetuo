package com.criteo.perpetuo.dao

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import spray.json.JsString

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class ExecutionTraceSpec
  extends FunSuite
    with ScalaFutures
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
    Await.result(
      for {
        product <- insertProductIfNotExists("perpetuo-app")
        request <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "No fear", "c.norris"))
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "c.norris"))
        execSpec <- insertExecutionSpecification("{}", Version("\"456\""))
        execId <- dbContext.db.run(insertExecution(deployOperationTrace.id, execSpec.id))
        execTraceIds <- dbContext.db.run(insertExecutionTraces(execId, 1))
        execTraces <- dbContext.db.run(executionTraceQuery.result)
      } yield {
        assert(execTraces.length == 1)
        assert(execTraces.head.id.get == execTraceIds.head)
        assert(execTraces.head.executionId == execId)
        assert(execTraces.head.logHref.isEmpty)
        assert(execTraces.head.state == ExecutionState.pending)
      },
      1.second
    )
  }
}
