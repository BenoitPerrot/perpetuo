package com.criteo.perpetuo.engine

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.model._
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class EngineSpec extends Test with TestDb {

  private val engine = new Engine(new DbBinding(dbContext))

  "Engine" should {
    "keep track of open executions for an operation" in {
      Await.result(
        for {
          product <- engine.insertProduct("human")
          deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("42").compactPrint), """["moon","mars"]}""", "", "robert", new Timestamp(System.currentTimeMillis)), immediateStart = false).map(_ ("id").toString.toLong)
          _ <- engine.startDeploymentRequest(deploymentRequestId, "ignace")
          operationTraces <- engine.findOperationTracesByDeploymentRequest(deploymentRequestId).map(_.get)
          operationTraceId = operationTraces.head.id
          hasOpenExecutionBefore <- engine.dbBinding.hasOpenExecutionTracesForOperation(operationTraceId)
          executionTrace <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).map(_.head)
          _ <- engine.updateExecutionTrace(executionTrace.id, ExecutionState.completed, "", Map(
            "moon" -> TargetAtomStatus(Status.success, "no surprise"),
            "mars" -> TargetAtomStatus(Status.hostFailure, "no surprise")))
          hasOpenExecutionAfter <- engine.dbBinding.hasOpenExecutionTracesForOperation(operationTraceId)
          operationReClosingSucceeded <- engine.dbBinding.closeOperationTrace(operationTraceId)
        } yield {
          (hasOpenExecutionBefore, hasOpenExecutionAfter, operationReClosingSucceeded)
        },
        2.second
      ) shouldBe(true, false, false)
    }

    def mockDeployExecution(productName: String, v: String, targetAtomToStatus: Map[String, Status.Code]): Future[(Long, Long)] = {
      for {
        deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(productName, Version(JsString(v).compactPrint), targetAtomToStatus.keys.toJson.compactPrint, "", "r.equestor", new Timestamp(System.currentTimeMillis)), immediateStart = false).map(_ ("id").toString.toLong)
        _ <- engine.startDeploymentRequest(deploymentRequestId, "s.tarter")
        executionTraceId <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).map(_.head).map(_.id)
        _ <- engine.updateExecutionTrace(executionTraceId, ExecutionState.completed, "", targetAtomToStatus.mapValues(c => TargetAtomStatus(c, "")))
      } yield {
        (deploymentRequestId, executionTraceId)
      }
    }

    "find executions for rolling back" in {
      Await.result(
        for {
          product <- engine.insertProduct("mice")

          (firstDeploymentRequestId, firstExecutionTraceId) <- mockDeployExecution(product.name, "27", Map("moon" -> Status.success, "mars" -> Status.success))
          // Status = moon: mice@27, mars: mice@27

          (secondDeploymentRequestId, secondExecutionTraceId) <- mockDeployExecution(product.name, "54", Map("moon" -> Status.success, "mars" -> Status.productFailure))
          // Status = moon: mice@54, mars: mice@27

          (thirdDeploymentRequestId, _) <- mockDeployExecution(product.name, "69", Map("moon" -> Status.success, "mars" -> Status.productFailure))
          // Status = moon: mice@69, mars: mice@27

          // Rolling back
          operationToRollback <- engine.dbBinding.findOperationTracesByDeploymentRequest(thirdDeploymentRequestId).map(_.head)
          executionIdsForRollback <- engine.dbBinding.findSoundExecutionIdsForRollback(operationToRollback)

        } yield {
          (executionIdsForRollback.size,
            executionIdsForRollback("mars") == firstExecutionTraceId,
            executionIdsForRollback("moon") == secondExecutionTraceId)
        },
        2.second
      ) shouldBe(2, true, true)
    }
  }

}
