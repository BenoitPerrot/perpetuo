package com.criteo.perpetuo.dao

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


@RunWith(classOf[JUnitRunner])
class TargetStatusSpec
  extends FunSuite
    with ScalaFutures
    with TargetStatusBinder
    with ExecutionTraceBinder
    with ExecutionBinder
    with ExecutionSpecificationBinder
    with OperationTraceBinder
    with DeploymentRequestBinder
    with ProductBinder
    with TestDb {

  import dbContext.driver.api._

  private def readStatuses: Future[String] =
    dbContext.db.run(targetStatusQuery.result).map(
      _.map(status => s"${status.targetAtom}: ${status.code} (${status.detail})").toList.sorted.mkString(", ")
    )

  test("Target statuses can be inserted and retrieved") {
    Await.result(
      for {
        _ <- dbContext.db.run(targetStatusQuery.delete)
        product <- insertProduct("perpetuo-app")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"v42\""), "Moon", "That's one small step for man, one giant leap for mankind", "n.armstrong"))
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "n.armstrong"))
        execSpec <- insertExecutionSpecification("{}", Version("\"456\""))
        execId <- dbContext.db.run(insertExecution(deployOperationTrace.id, execSpec.id))
        _ <- dbContext.db.run(updateTargetStatuses(execId, Map(
          "Moon" -> TargetAtomStatus(Status.hostFailure, "Houston, we've got a problem"))))
        targetStatuses <- readStatuses
        nbStatuses <- dbContext.db.run(targetStatusQuery.filter(_.executionId === execId).length.result)
      } yield {
        assert(targetStatuses == "Moon: hostFailure (Houston, we've got a problem)")
        assert(nbStatuses == 1)
      },
      1.second
    )
  }

  test("Target statuses can be updated") {
    Await.result(
      for {
        _ <- dbContext.db.run(targetStatusQuery.delete)
        product <- insertProduct("sleepy-owl")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"0\""), "Earth", "", "creator"))
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "runner"))
        execSpec <- insertExecutionSpecification("{}", Version("\"0\""))
        execId <- dbContext.db.run(insertExecution(deployOperationTrace.id, execSpec.id))
        statuses1 <- readStatuses
        _ <- dbContext.db.run(updateTargetStatuses(execId, Map(
          "West" -> TargetAtomStatus(Status.running, "confident"))))
        statuses2 <- readStatuses
        _ <- dbContext.db.run(updateTargetStatuses(execId, Map(
          "West" -> TargetAtomStatus(Status.productFailure, "crashing"),
          "East" -> TargetAtomStatus(Status.running, "starting"))))
        statuses3 <- readStatuses
        _ <- dbContext.db.run(updateTargetStatuses(execId, Map(
          "East" -> TargetAtomStatus(Status.running, "soaring"))))
        statuses4 <- readStatuses
      } yield {
        assert(statuses1 == "")
        assert(statuses2 == "West: running (confident)")
        assert(statuses3 == "East: running (starting), West: productFailure (crashing)")
        assert(statuses4 == "East: running (soaring), West: productFailure (crashing)")
      },
      1.second
    )
  }
}
