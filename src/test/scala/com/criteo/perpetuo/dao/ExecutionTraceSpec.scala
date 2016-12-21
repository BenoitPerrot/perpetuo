package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.dao.enums.{ExecutionState, Operation, TargetStatus}
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
  with DeploymentTraceBinder with DeploymentRequestBinder
  with ProfileProvider {

  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  val config: Config = ConfigFactory.load()
  val dbModule = new TestingDbContextModule(config.getConfig("db").getConfig("embedded"))

  val profile: JdbcDriver = dbModule.driver

  import profile.api._

  val db: profile.backend.DatabaseDef = Database.forDataSource(dbModule.dataSourceProvider)

  val schemaCreation = DBIO.seq(
    deploymentRequestQuery.schema.create,
    deploymentTraceQuery.schema.create,
    executionTraceQuery.schema.create
  )
  Await.result(db.run(schemaCreation), 2.second)

  test("ExecutionState values are all different") {
    ExecutionState.values
  }

  test("Execution traces can be bound to deployment traces, and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(for {
      requestId <- insert(db, request)
      deployId <- addToDeploymentRequest(db, requestId, Operation.deploy)
      execId <- addToDeploymentTrace(db, deployId)
      execTraces <- db.run(executionTraceQuery.result)
      execTrace <- findExecutionTraceById(db, execId)
    } yield {
      assert(execTrace.isDefined)
      assert(execTraces == Seq(execTrace.get))
      assert(execTrace.get.id.get == execId)
      assert(execTrace.get.deploymentTraceId == deployId)
      assert(execTrace.get.guid == "")
      assert(execTrace.get.state == ExecutionState.pending)
    }, 2.second)
  }
}
