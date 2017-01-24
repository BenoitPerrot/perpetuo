package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.app.{AppConfig, DbContext}
import com.criteo.perpetuo.dao.enums.{ExecutionState, Operation}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class ExecutionTraceSpec extends FunSuite with ScalaFutures
  with ExecutionTraceBinder
  with OperationTraceBinder with DeploymentRequestBinder
  with DbContextProvider {

  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  private val dbModule = new TestingDbContextModule(AppConfig.withEnv("test").db)
  val dbContext: DbContext = dbModule.providesDbContext
  import dbContext.driver.api._

  test("ExecutionState values are all different") {
    ExecutionState.values
  }

  test("Execution traces can be bound to operation traces, and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(for {
      requestId <- insert(request)
      deployId <- addToDeploymentRequest(requestId, Operation.deploy)
      execIds <- addToOperationTrace(deployId, 1)
      execTraces <- dbContext.db.run(executionTraceQuery.result)
      execTrace <- findExecutionTraceById(execIds.head)
    } yield {
      assert(execTrace.isDefined)
      assert(execTraces == Seq(execTrace.get))
      assert(execTrace.get.id.get == execIds.head)
      assert(execTrace.get.operationTraceId == deployId)
      assert(execTrace.get.uuid.isEmpty)
      assert(execTrace.get.state == ExecutionState.pending)
    }, 2.seconds)
  }
}
