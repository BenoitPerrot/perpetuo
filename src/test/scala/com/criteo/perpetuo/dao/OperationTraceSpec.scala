package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.app.{AppConfig, DbContext}
import com.criteo.perpetuo.dao.enums.{Operation, TargetStatus}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class OperationTraceSpec extends FunSuite with ScalaFutures
  with OperationTraceBinder
  with DeploymentRequestBinder
  with DbContextProvider {

  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  private val dbModule = new TestingDbContextModule(AppConfig.withEnv("test").db)
  val dbContext: DbContext = dbModule.providesDbContext
  import dbContext.driver.api._

  test("Operation types are bound to different integral values") {
    Operation.values
  }

  test("TargetStatus values are all different") {
    TargetStatus.values
  }

  test("Operation traces can be bound to deployment requests, and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(for {
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
    }, 2.seconds)
  }

  test("Operation traces can serialize and de-serialize a target status") {
    // using the same records already inserted in the DB during the test above

    Await.result(for {
      traceId <- dbContext.db.run(operationTraceQuery.result).map(_.head.id.get)
      _ <- updateOperationTrace(traceId, Map("abc" -> TargetStatus.serverFailure))
      trace <- findOperationTraceById(traceId)
    } yield {
      assert(trace.get.targetStatus == Map("abc" -> TargetStatus.serverFailure))
    }, 2.seconds)
  }
}
