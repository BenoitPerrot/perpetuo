package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.dao.enums.{ExecutionState, Operation}
import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}
import slick.driver.JdbcDriver

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class ExecutionTraceSpec extends FunSuite with ScalaFutures
  with ExecutionTraceBinder
  with OperationTraceBinder with DeploymentRequestBinder
  with ProfileProvider {

  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  private val config: Config = ConfigFactory.load()
  private val dbModule = new TestingDbContextModule(config.getConfig("db").getConfig("test"))

  val profile: JdbcDriver = dbModule.driver
  import profile.api._

  private val db = Database.forDataSource(dbModule.dataSourceProvider)
  new Schema(profile).createTables(db)

  test("ExecutionState values are all different") {
    ExecutionState.values
  }

  test("Execution traces can be bound to operation traces, and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(for {
      requestId <- insert(db, request)
      deployId <- addToDeploymentRequest(db, requestId, Operation.deploy)
      execIds <- addToOperationTrace(db, deployId, 1)
      execTraces <- db.run(executionTraceQuery.result)
      execTrace <- findExecutionTraceById(db, execIds.head)
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
