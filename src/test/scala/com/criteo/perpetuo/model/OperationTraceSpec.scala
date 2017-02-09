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
    TargetStatus.values
  }

  test("Operation traces can be bound to deployment requests, and retrieved") {
    Await.result(
      for {
        product <- insert("perpetuo-app")
        request <- insert(new DeploymentRequestAttrs(product.name, "v42", "*", "No fear", "c.norris", new Timestamp(123456789)))
        deployId <- addToDeploymentRequest(request.id, Operation.deploy)
        revertId <- addToDeploymentRequest(request.id, Operation.revert)
        traces <- dbContext.db.run(operationTraceQuery.result)
        deploy <- findOperationTraceById(deployId)
        revert <- findOperationTraceById(revertId)
      } yield {
        assert(deploy.isDefined && revert.isDefined)
        assert(traces == Seq(deploy.get, revert.get))
        assert(deploy.get.id.get != revert.get.id.get) // different primary keys
        assert(deploy.get.deploymentRequestId == revert.get.deploymentRequestId) // same foreign key
        assert(deploy.get.deploymentRequestId == request.id) // pointing to the same DeploymentRequest
        assert(deploy.get.operation == Operation.deploy) // right operation type
        assert(revert.get.operation == Operation.revert) // right operation type
        assert(deploy.get.operation != revert.get.operation) // different operation types
        assert(deploy.get.targetStatus == revert.get.targetStatus) // same target status
        assert(deploy.get.targetStatus == Map()) // same empty target status
      },
      1.second
    )
  }

  test("Operation traces can serialize and de-serialize a target status") {
    // using the same records already inserted in the DB during the test above

    Await.result(
      for {
        traceId <- dbContext.db.run(operationTraceQuery.result).map(_.head.id.get)
        _ <- updateOperationTrace(traceId, Map("abc" -> TargetStatus.serverFailure))
        trace <- findOperationTraceById(traceId)
      } yield {
        assert(trace.get.targetStatus == Map("abc" -> TargetStatus.serverFailure))
      },
      1.second
    )
  }
}
