package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, ExecutionState, Operation, Version}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class ExecutionTraceSpec extends FunSuite with ScalaFutures
  with ExecutionTraceBinder
  with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
  with TestDb {

  import dbContext.driver.api._

  test("ExecutionState values are all different") {
    ExecutionState.values
  }

  test("Execution traces can be bound to operation traces, and retrieved") {
    Await.result(
      for {
        product <- insertProduct("perpetuo-app")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"v42\""), "*", "No fear", "c.norris", new Timestamp(123456789)))
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request.id, Operation.deploy, "c.norris"))
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
