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
          executionIdsForRollback <- engine.dbBinding.findExecutionIdsForRollback(operationToRollback)

        } yield {
          (executionIdsForRollback.size,
            executionIdsForRollback("mars") == Some(firstExecutionTraceId),
            executionIdsForRollback("moon") == Some(secondExecutionTraceId))
        },
        2.second
      ) shouldBe(2, true, true)
    }

    "identify that the very first operation cannot be rolled back" in {
      Await.result(
        for {
          product <- engine.insertProduct("monkey")

          (firstDeploymentRequestId, firstExecutionTraceId) <- mockDeployExecution(product.name, "12", Map("orbit" -> Status.success))
          // Status = orbit: monkey@12

          (secondDeploymentRequestId, secondExecutionTraceId) <- mockDeployExecution(product.name, "55", Map("orbit" -> Status.success, "venus" -> Status.hostFailure))
          // Status = orbit: monkey@55, venus: (none)

          (thirdDeploymentRequestId, thirdExecutionTraceId) <- mockDeployExecution(product.name, "69", Map("orbit" -> Status.success, "venus" -> Status.success))
          // Status = orbit: monkey@69, venus: monkey@69

          // Rolling back
          operationToRollback <- engine.dbBinding.findOperationTracesByDeploymentRequest(thirdDeploymentRequestId).map(_.head)
          executionIdsForRollback <- engine.dbBinding.findExecutionIdsForRollback(operationToRollback)

        } yield {
          (executionIdsForRollback.size,
            executionIdsForRollback("orbit") == Some(secondExecutionTraceId),
            executionIdsForRollback("venus").isEmpty)
        },
        2.second
      ) shouldBe(2, true, true)
    }

    "keep track of retried operation" in {
      Await.result(
        for {
          product <- engine.insertProduct("martian")
          deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("42").compactPrint), """["moon","mars"]}""", "", "robert", new Timestamp(System.currentTimeMillis)), immediateStart = false).map(_ ("id").toString.toLong)
          _ <- engine.startDeploymentRequest(deploymentRequestId, "ignace")
          operationTraces <- engine.findOperationTracesByDeploymentRequest(deploymentRequestId).map(_.get)
          operationTraceId = operationTraces.head.id
          executionTrace <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId)
          _ <- engine.updateExecutionTrace(executionTrace.head.id, ExecutionState.completed, "", Map(
            "moon" -> TargetAtomStatus(Status.success, "no surprise"),
            "mars" -> TargetAtomStatus(Status.hostFailure, "no surprise")))
          retriedOperation <- engine.retryOperationTrace(operationTraceId, "b.lightning").map(_.get)
          executionTracesAfterRetry <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId)
          _ <- engine.updateExecutionTrace(executionTracesAfterRetry.tail.head.id, ExecutionState.completed, "", Map(
            "moon" -> TargetAtomStatus(Status.success, "no surprise"),
            "mars" -> TargetAtomStatus(Status.success, "no surprise")))
          hasOpenExecutionAfter <- engine.dbBinding.hasOpenExecutionTracesForOperation(retriedOperation.id)
          operationReClosingSucceeded <- engine.dbBinding.closeOperationTrace(retriedOperation.id)
          (_, initialExecutionSpecs) <- engine.dbBinding.findOperationTraceAndExecutionSpecs(operationTraceId).map(_.get)
          (_, retriedExecutionSpecs) <- engine.dbBinding.findOperationTraceAndExecutionSpecs(retriedOperation.id).map(_.get)
        } yield {
          (executionTrace.length, executionTracesAfterRetry.length,
            retriedOperation.id == operationTraceId,
            hasOpenExecutionAfter, operationReClosingSucceeded,
            initialExecutionSpecs.length == retriedExecutionSpecs.length,
            initialExecutionSpecs == retriedExecutionSpecs
          )
        },
        2.second
      ) shouldBe (1, 2, false, false, false, true, true)
    }
  }

}
