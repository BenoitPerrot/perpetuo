package com.criteo.perpetuo.engine

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.model._
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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
        } yield (
          hasOpenExecutionBefore, hasOpenExecutionAfter, operationReClosingSucceeded
        ),
        2.seconds
      ) shouldBe(true, false, false)
    }

    def mockDeployExecution(productName: String, v: String, targetAtomToStatus: Map[String, Status.Code]): Future[(Long, Long)] = {
      for {
        deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(productName, Version(JsString(v).compactPrint), targetAtomToStatus.keys.toJson.compactPrint, "", "r.equestor", new Timestamp(System.currentTimeMillis)), immediateStart = false).map(_ ("id").toString.toLong)
        _ <- engine.startDeploymentRequest(deploymentRequestId, "s.tarter")
        executionTrace <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).map(_.head)
        executionSpecId <- engine.dbBinding.findExecutionSpecificationId(executionTrace.executionId)
        _ <- engine.updateExecutionTrace(executionTrace.id, ExecutionState.completed, "", targetAtomToStatus.mapValues(c => TargetAtomStatus(c, "")))
      } yield (deploymentRequestId, executionSpecId.get)
    }

    "check if an operation can be retried" in {
      Await.result(
        for {
          product <- engine.insertProduct("horse")

          (firstDeploymentRequestId, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.hostFailure))
          // Status = corn-field: horse@100?

          (secondDeploymentRequestId, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success, "pool" -> Status.productFailure))
          // Status = pool: horse@101?, racing: horse@101, corn-field: horse@100?

          (thirdDeploymentRequestId, _) <- mockDeployExecution(product.name, "102", Map("racing" -> Status.success))
          // Status = pool: horse@101?, racing: horse@102, corn-field: horse@100?

          // fixme: one day, first deployment will be retryable:
          // But second one can't be, because it impacts `racing`, whose status changed in the meantime
          secondDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
          rejectionOfSecond <- engine.actionChecker(secondDeploymentRequest, isStarted = true)(Action.applyAgain).failed
          // The last one of course is retryable
          thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
          _ <- engine.actionChecker(thirdDeploymentRequest, isStarted = true)(Action.applyAgain)
        } yield rejectionOfSecond.getMessage,
        2.seconds
      ) shouldBe "a newer one has already been applied"
    }

    "find executions for rolling back" in {
      Await.result(
        for {
          product <- engine.insertProduct("mouse")

          (firstDeploymentRequestId, firstExecSpecId) <- mockDeployExecution(product.name, "27", Map("moon" -> Status.success, "mars" -> Status.success))
          // Status = moon: mouse@27, mars: mouse@27

          (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "54", Map("moon" -> Status.productFailure, "venus" -> Status.success))
          // Status = moon: mouse@54, mars: mouse@27

          (thirdDeploymentRequestId, thirdExecSpecId) <- mockDeployExecution(product.name, "69", Map("moon" -> Status.success, "mars" -> Status.productFailure))
          // Status = moon: mouse@69, mars: mouse@27

          // Rolling back
          thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
          executionSpecsForRollback <- engine.dbBinding.findExecutionSpecificationsForRollback(thirdDeploymentRequest)

        } yield (
          executionSpecsForRollback.size,
          executionSpecsForRollback("mars").get.id == firstExecSpecId,
          executionSpecsForRollback("moon").get.id == secondExecSpecId,
          executionSpecsForRollback("mars").get.version.toString,
          executionSpecsForRollback("moon").get.version.toString
        ),
        2.seconds
      ) shouldBe(2, true, true, """"27"""", """"54"""")
    }

    "check if an operation can be rolled back" in {
      Await.result(
        for {
          product <- engine.insertProduct("monkey")
          otherProduct <- engine.insertProduct("donkey")

          _ <- mockDeployExecution(product.name, "12", Map("orbit" -> Status.success))
          // Status = orbit: monkey@12

          (secondDeploymentRequestId, _) <- mockDeployExecution(product.name, "55", Map("orbit" -> Status.success, "venus" -> Status.hostFailure))
          // Status = orbit: monkey@55, venus: (none)

          (thirdDeploymentRequestId, _) <- mockDeployExecution(product.name, "69", Map("orbit" -> Status.success, "venus" -> Status.success))
          // Status = orbit: monkey@69, venus: monkey@69

          (otherDeploymentRequestId, _) <- mockDeployExecution(otherProduct.name, "215", Map("orbit" -> Status.success, "venus" -> Status.success))
          // Status = orbit: monkey@69 + bonobo@215, venus: monkey@69 + bonobo@215

          // Second request can't be rolled back
          secondDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
          rejectionOfSecond <- engine.actionChecker(secondDeploymentRequest, isStarted = true)(Action.rollback).failed
          // Third one can be rolled back, because it's the last one for its product
          thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
          _ <- engine.actionChecker(thirdDeploymentRequest, isStarted = true)(Action.rollback)
          // Meanwhile the deployment on another product can't be rolled back because it's the first for that product, even though it's globally the last
          otherDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(otherDeploymentRequestId).map(_.get)
          rejectionOfOther <- engine.actionChecker(otherDeploymentRequest, isStarted = true)(Action.rollback).failed

        } yield (
          rejectionOfSecond.getMessage, rejectionOfOther.getMessage.slice(0, 22)
        ),
        2.seconds
      ) shouldBe("a newer one has already been applied", "unknown previous state")
    }

    "perform a roll back" in {
      Await.result(
        for {
          product <- engine.insertProduct("pony")

          (firstDeploymentRequestId, firstExecSpecId) <- mockDeployExecution(product.name, "11", Map("tic" -> Status.success, "tac" -> Status.productFailure))
          // Status = tic: pony@11, tac: pony@11

          (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "22", Map("tic" -> Status.success, "tac" -> Status.success))
          // Status = tic: pony@22, tac: pony@11

          // Rolling back
          rollbackOperationTrace <- engine.rollbackDeploymentRequest(secondDeploymentRequestId, "r.ollbacker").map(_.get)
          rollbackExecutionSpecIds <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(rollbackOperationTrace.id)

        } yield (
          rollbackOperationTrace.deploymentRequestId == secondDeploymentRequestId,
          rollbackExecutionSpecIds.length,
          rollbackExecutionSpecIds.contains(firstExecSpecId)
        ),
        2.seconds
      ) shouldBe(true, 1, true)
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
          retriedOperation <- engine.deployAgain(deploymentRequestId, "b.lightning").map(_.get)
          executionTracesAfterRetry <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId)
          _ <- engine.updateExecutionTrace(executionTracesAfterRetry.tail.head.id, ExecutionState.completed, "", Map(
            "moon" -> TargetAtomStatus(Status.success, "no surprise"),
            "mars" -> TargetAtomStatus(Status.success, "no surprise")))
          hasOpenExecutionAfter <- engine.dbBinding.hasOpenExecutionTracesForOperation(retriedOperation.id)
          operationReClosingSucceeded <- engine.dbBinding.closeOperationTrace(retriedOperation.id)
          initialExecutionSpecIds <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(operationTraceId)
          retriedExecutionSpecIds <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(retriedOperation.id)
        } yield (
          executionTrace.length,
          executionTracesAfterRetry.length,
          retriedOperation.id == operationTraceId,
          hasOpenExecutionAfter, operationReClosingSucceeded,
          initialExecutionSpecIds.length == retriedExecutionSpecIds.length,
          initialExecutionSpecIds == retriedExecutionSpecIds
        ),
        2.seconds
      ) shouldBe(1, 2, false, false, false, true, true)
    }
  }

}
