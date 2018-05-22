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
class OperationTraceSpec
  extends FunSuite
    with ScalaFutures
    with OperationTraceBinder
    with DeploymentRequestBinder
    with ProductBinder
    with DeploymentRequestInserter
    with TestDb {

  import dbContext.profile.api._

  test("Operation types are bound to different integral values") {
    Operation.values
  }

  test("TargetStatus values are all different") {
    Status.values
  }

  test("Operation traces can be bound to deployment requests, and retrieved") {
    Await.result(
      for {
        product <- insertProductIfNotExists("perpetuo-app")
        request <- insertDeploymentRequest(DeploymentRequestAttrs(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "No fear", "c.norris"))
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "c.norris"))
        revertOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.revert, "c.norris"))
        traces <- dbContext.db.run(operationTraceQuery.result)
      } yield {
        assert(traces.length == 2)
        val deploy = traces.head
        val revert = traces.tail.head
        assert(deployOperationTrace.id == deploy.id.get)
        assert(revertOperationTrace.id == revert.id.get)
        assert(deploy.id.get != revert.id.get) // different primary keys
        assert(deploy.deploymentRequestId == revert.deploymentRequestId) // same foreign key
        assert(deploy.deploymentRequestId == request.id) // pointing to the same DeploymentRequest
        assert(deploy.operation == Operation.deploy) // right operation type
        assert(revert.operation == Operation.revert) // right operation type
        assert(deploy.operation != revert.operation) // different operation types
      },
      1.second
    )
  }
}
