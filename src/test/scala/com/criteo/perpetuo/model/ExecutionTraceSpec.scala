package com.criteo.perpetuo.model

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.{DeploymentRequestBinder, ExecutionTraceBinder, OperationTraceBinder, ProductBinder}
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
  with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
  with TestDb {

  import dbContext.driver.api._

  test("ExecutionState values are all different") {
    ExecutionState.values
  }

  test("Execution traces can be bound to operation traces, and retrieved") {
    Await.result(
      for {
        product <- insert("perpetuo-app")
        request <- insert(new DeploymentRequestAttrs(product.name, "v42", "*", "No fear", "c.norris", new Timestamp(123456789)))
        deployId <- addToDeploymentRequest(request.id, Operation.deploy)
        execIds <- addToOperationTrace(deployId, 1)
        execTraces <- dbContext.db.run(executionTraceQuery.result)
        execTrace <- findExecutionTraceById(execIds.head)
      } yield {
        assert(execTrace.isDefined)
        assert(execTraces == Seq(execTrace.get))
        assert(execTrace.get.id.get == execIds.head)
        assert(execTrace.get.operationTraceId == deployId)
        assert(execTrace.get.logHref.isEmpty)
        assert(execTrace.get.state == ExecutionState.pending)
      },
      1.second
    )
  }
}
