package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json.JsString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class TargetStatusSpec
  extends TestHelpers
    with TargetStatusBinder
    with ExecutionTraceBinder
    with ExecutionBinder
    with ExecutionSpecificationBinder
    with OperationTraceBinder
    with DeploymentRequestBinder
    with ProductBinder
    with DeploymentRequestInserter
    with TestDb {

  import dbContext.profile.api._

  private def readStatuses: Future[String] =
    dbContext.db.run(targetStatusQuery.result).map(
      _.map(status => s"${status.targetAtom}: ${status.code} (${status.detail})").toList.sorted.mkString(", ")
    )

  test("Target statuses can be inserted and retrieved") {
    await(
      for {
        _ <- dbContext.db.run(targetStatusQuery.delete)
        product <- upsertProduct("perpetuo-app")
        request <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", JsString("Moon"), "")), "That's one small step for man, one giant leap for mankind", "n.armstrong")).map(_.deploymentRequest)
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "n.armstrong"))
        execSpec <- insertExecutionSpecification("{}", Version("\"456\""))
        execId <- dbContext.db.run(insertExecution(deployOperationTrace.id, execSpec.id))
        _ <- dbContext.db.run(updatingTargetStatuses(execId, Map(
          "Moon" -> TargetAtomStatus(Status.hostFailure, "Houston, we've got a problem"))))
        targetStatuses <- readStatuses
        nbStatuses <- dbContext.db.run(targetStatusQuery.filter(_.executionId === execId).length.result)
      } yield {
        targetStatuses shouldEqual "Moon: hostFailure (Houston, we've got a problem)"
        nbStatuses shouldEqual 1
      }
    )
  }

  test("Target statuses can be updated") {
    await(
      for {
        _ <- dbContext.db.run(targetStatusQuery.delete)
        product <- upsertProduct("sleepy-owl")
        request <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"0\""), Seq(ProtoDeploymentPlanStep("", JsString("Earth"), "")), "", "creator")).map(_.deploymentRequest)
        deployOperationTrace <- dbContext.db.run(insertOperationTrace(request, Operation.deploy, "runner"))
        execSpec <- insertExecutionSpecification("{}", Version("\"0\""))
        execId <- dbContext.db.run(insertExecution(deployOperationTrace.id, execSpec.id))
        statuses1 <- readStatuses
        _ <- dbContext.db.run(updatingTargetStatuses(execId, Map(
          "West" -> TargetAtomStatus(Status.running, "confident"))))
        statuses2 <- readStatuses
        _ <- dbContext.db.run(updatingTargetStatuses(execId, Map(
          "West" -> TargetAtomStatus(Status.productFailure, "crashing"),
          "East" -> TargetAtomStatus(Status.running, "starting"))))
        statuses3 <- readStatuses
        _ <- dbContext.db.run(updatingTargetStatuses(execId, Map(
          "East" -> TargetAtomStatus(Status.running, "soaring"))))
        statuses4 <- readStatuses
      } yield {
        statuses1 shouldEqual ""
        statuses2 shouldEqual "West: running (confident)"
        statuses3 shouldEqual "East: running (starting), West: productFailure (crashing)"
        statuses4 shouldEqual "East: running (soaring), West: productFailure (crashing)"
      }
    )
  }
}
