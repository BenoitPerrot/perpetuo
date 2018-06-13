package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json.JsString

import scala.concurrent.ExecutionContext.Implicits.global


class OperationTraceSpec
  extends TestHelpers
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
    await(
      for {
        product <- insertProductIfNotExists("perpetuo-app")
        request <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "No fear", "c.norris")).map(_.deploymentRequest)
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "c.norris"))
        revertOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.revert, "c.norris"))
        traces <- dbContext.db.run(operationTraceQuery.result)
      } yield {
        traces.length shouldEqual 2
        val deploy = traces.head
        val revert = traces.tail.head
        deployOperationTrace.id shouldEqual deploy.id.get
        revertOperationTrace.id shouldEqual revert.id.get
        deploy.id.get shouldNot equal(revert.id.get) // different primary keys
        deploy.deploymentRequestId shouldEqual revert.deploymentRequestId // same foreign key
        deploy.deploymentRequestId shouldEqual request.id // pointing to the same DeploymentRequest
        deploy.operation shouldEqual Operation.deploy // right operation type
        revert.operation shouldEqual Operation.revert // right operation type
        deploy.operation shouldNot equal(revert.operation) // different operation types
      }
    )
  }
}
