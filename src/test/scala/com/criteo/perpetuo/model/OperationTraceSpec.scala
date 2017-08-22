package com.criteo.perpetuo.model

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.{DeploymentRequestBinder, OperationTraceBinder, ProductBinder}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class OperationTraceSpec extends FunSuite with ScalaFutures
  with OperationTraceBinder
  with DeploymentRequestBinder with ProductBinder
  with TestDb {

  import dbContext.driver.api._

  test("Operation types are bound to different integral values") {
    Operation.values
  }

  test("TargetStatus values are all different") {
    Status.values
  }

  test("Operation traces can be bound to deployment requests, and retrieved") {
    Await.result(
      for {
        product <- insertProduct("perpetuo-app")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"v42\""), "*", "No fear", "c.norris", new Timestamp(123456789)))
        deployOperationTrace <- insertOperationTrace(request.id, Operation.deploy, "c.norris")
        revertOperationTrace <- insertOperationTrace(request.id, Operation.revert, "c.norris")
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
        assert(deploy.targetStatus.get == revert.targetStatus.get) // same target status
        assert(deploy.targetStatus.get == Map()) // same empty target status
      },
      1.second
    )
  }

  test("Operation traces can serialize and de-serialize a target status") {
    // using the same records already inserted in the DB during the test above

    Await.result(
      for {
        traceId <- dbContext.db.run(operationTraceQuery.result).map(_.head.id.get)
        updated <- updateOperationTrace(traceId, Map("abc" -> TargetAtomStatus(Status.hostFailure, "details...")))
        traces <- dbContext.db.run(operationTraceQuery.result)
      } yield {
        assert(updated)
        assert(traces.head.targetStatus.get == Map("abc" -> TargetAtomStatus(Status.hostFailure, "details...")))
      },
      1.second
    )
  }
}
