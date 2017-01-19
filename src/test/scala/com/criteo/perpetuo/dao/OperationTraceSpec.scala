package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.dao.enums.{Operation, TargetStatus}
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
class OperationTraceSpec extends FunSuite with ScalaFutures
  with OperationTraceBinder
  with DeploymentRequestBinder
  with ProfileProvider {

  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  private val config: Config = ConfigFactory.load()
  private val dbModule = new TestingDbContextModule(config.getConfig("db").getConfig("test"))

  val profile: JdbcDriver = dbModule.driver
  import profile.api._

  private val db = Database.forDataSource(dbModule.dataSourceProvider)
  new Schema(profile).createTables(db)

  test("Operation types are bound to different integral values") {
    Operation.values
  }

  test("TargetStatus values are all different") {
    TargetStatus.values
  }

  test("Operation traces can be bound to deployment requests, and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(for {
      requestId <- insert(db, request)
      deployId <- addToDeploymentRequest(db, requestId, Operation.deploy)
      revertId <- addToDeploymentRequest(db, requestId, Operation.revert)
      traces <- db.run(operationTraceQuery.result)
      deploy <- findOperationTraceById(db, deployId)
      revert <- findOperationTraceById(db, revertId)
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
      traceId <- db.run(operationTraceQuery.result).map(_.head.id.get)
      _ <- updateOperationTrace(db, traceId, Map("abc" -> TargetStatus.serverFailure))
      trace <- findOperationTraceById(db, traceId)
    } yield {
      assert(trace.get.targetStatus == Map("abc" -> TargetStatus.serverFailure))
    }, 2.seconds)
  }
}
