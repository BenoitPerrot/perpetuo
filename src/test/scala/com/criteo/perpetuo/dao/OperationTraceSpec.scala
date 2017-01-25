package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.enums.{Operation, TargetStatus}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global


@RunWith(classOf[JUnitRunner])
class OperationTraceSpec extends FunSuite with ScalaFutures
  with OperationTraceBinder
  with DeploymentRequestBinder
  with TestDb
  with Eventually {

  implicit val defaultPatience = PatienceConfig(timeout = Span(1, Seconds), interval = Span(100, Millis))

  import dbContext.driver.api._

  test("Operation types are bound to different integral values") {
    Operation.values
  }

  test("TargetStatus values are all different") {
    TargetStatus.values
  }

  test("Operation traces can be bound to deployment requests, and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    eventually {
      for {
        requestId <- insert(request)
        deployId <- addToDeploymentRequest(requestId, Operation.deploy)
        revertId <- addToDeploymentRequest(requestId, Operation.revert)
        traces <- dbContext.db.run(operationTraceQuery.result)
        deploy <- findOperationTraceById(deployId)
        revert <- findOperationTraceById(revertId)
      } yield {
        assert(deploy.isDefined && revert.isDefined)
        assert(traces == Seq(deploy.get, revert.get))
        assert(deploy.get.id.get != revert.get.id.get) // different primary keys
        assert(deploy.get.deploymentRequestId == revert.get.deploymentRequestId) // same foreign key
        assert(deploy.get.deploymentRequestId == requestId) // pointing to the same DeploymentRequest
        assert(deploy.get.operation == Operation.deploy) // right operation type
        assert(revert.get.operation == Operation.revert) // right operation type
        assert(deploy.get.operation != revert.get.operation) // different operation types
        assert(deploy.get.targetStatus == revert.get.targetStatus) // same target status
        assert(deploy.get.targetStatus == Map()) // same empty target status
      }
    }
  }

  test("Operation traces can serialize and de-serialize a target status") {
    // using the same records already inserted in the DB during the test above

    eventually {
      for {
        traceId <- dbContext.db.run(operationTraceQuery.result).map(_.head.id.get)
        _ <- updateOperationTrace(traceId, Map("abc" -> TargetStatus.serverFailure))
        trace <- findOperationTraceById(traceId)
      } yield {
        assert(trace.get.targetStatus == Map("abc" -> TargetStatus.serverFailure))
      }
    }
  }
}
