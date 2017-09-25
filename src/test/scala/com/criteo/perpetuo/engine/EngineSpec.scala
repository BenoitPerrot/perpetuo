package com.criteo.perpetuo.engine

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.config.Plugins
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.model._
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class EngineSpec extends Test with TestDb {

  private val plugins = new Plugins()
  private val engine = new Engine(new DbBinding(dbContext), plugins.dispatcher, plugins.permissions, plugins.listener)

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
          _ <- engine.updateExecutionTrace(executionTrace.id, ExecutionState.completed, "", "", Map(
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

    def mockDeployExecution(productName: String, v: String, targetAtomToStatus: Map[String, Status.Code], initFailure: Option[String] = None): Future[(Long, Long)] = {
      for {
        deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(productName, Version(JsString(v).compactPrint), targetAtomToStatus.keys.toJson.compactPrint, "", "r.equestor", new Timestamp(System.currentTimeMillis)), immediateStart = false).map(_ ("id").toString.toLong)
        operationTraceId <- engine.startDeploymentRequest(deploymentRequestId, "s.tarter").map(_.get.id)
        executionTraceIds <- engine.dbBinding.findExecutionTraceIdsByOperationTrace(operationTraceId)
        executionSpecIds <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(operationTraceId)
        _ <- engine.updateExecutionTrace(executionTraceIds.head, initFailure.map(_ => ExecutionState.initFailed).getOrElse(ExecutionState.completed), initFailure.getOrElse(""), "", targetAtomToStatus.mapValues(c => TargetAtomStatus(c, "")))
      } yield (deploymentRequestId, executionSpecIds.head)
    }

    def mockRollbackExecution(deploymentRequestId: Long, targetAtomToStatus: Map[String, Status.Code], defaultVersion: Option[Version] = None): Future[Long] = {
      for {
        operationTraceId <- engine.rollbackDeploymentRequest(deploymentRequestId, "r.everter", defaultVersion).map(_.get.id)
        executionTraceIds <- engine.dbBinding.findExecutionTraceIdsByOperationTrace(operationTraceId)
        _ <- Future.traverse(executionTraceIds)(
          engine.updateExecutionTrace(_, ExecutionState.completed, "", "", targetAtomToStatus.mapValues(c => TargetAtomStatus(c, "")))
        )
      } yield operationTraceId
    }

    "check that an operation can be started only if previous transactions on the same product have been completed" in {
      Await.result(
        for {
          product <- engine.insertProduct("pig")

          // OK if it's the first
          _ <- mockDeployExecution(product.name, "99", Map("racing" -> Status.success))

          // OK after a success
          (secondId, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.hostFailure))

          // not OK if it's after a deployment failure
          conflictMsg <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))
            .map(_ => "unrejected").recover { case UnprocessableAction(msg, _) => msg }

          // the failing one must be reverted first
          _ <- mockRollbackExecution(secondId, Map("corn-field" -> Status.hostFailure), Some(Version(""""big-bang"""")))

          // OK after the failing has been reverted (even if the revert failed)
          (thirdId, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))

          // note that we can revert a successful operation
          _ <- mockRollbackExecution(thirdId, Map("racing" -> Status.success))

          // OK after a revert of a successful operation
          _ <- mockDeployExecution(product.name, "102", Map("corn-field" -> Status.notDone), Some("crashed at start"))

          // OK after a init failure
          _ <- mockDeployExecution(product.name, "103", Map("racing" -> Status.success))
        } yield {
          conflictMsg shouldBe s"deployment request #$secondId has been left in an uncertain state, complete it first"
        },
        2.seconds
      )
    }

    "check if an operation can be retried" in {
      Await.result(
        for {
          product <- engine.insertProduct("horse")

          (firstDeploymentRequestId, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.success))
          // Status = corn-field: horse@100

          (secondDeploymentRequestId, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success, "pool" -> Status.success))
          // Status = corn-field: horse@100, racing: horse@101, pool: horse@101

          (thirdDeploymentRequestId, _) <- mockDeployExecution(product.name, "102", Map("racing" -> Status.productFailure))
          // Status = corn-field: horse@100, racing: horse@102?, pool: horse@101

          // fixme: one day, first deployment will be retryable:
          // But second one can't be, because it impacts `racing`, whose status changed in the meantime
          secondDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
          rejectionOfSecond <- engine.canDeployDeploymentRequest(secondDeploymentRequest).failed
          // The last one of course is retryable
          thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
          _ <- engine.canDeployDeploymentRequest(thirdDeploymentRequest)
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

          (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "54", Map("moon" -> Status.success, "venus" -> Status.success))
          // Status = moon: mouse@54, mars: mouse@27, venus: mouse@54

          (thirdDeploymentRequestId, thirdExecSpecId) <- mockDeployExecution(product.name, "69", Map("moon" -> Status.success, "mars" -> Status.productFailure))
          // Status = moon: mouse@69, mars: mouse@69?, venus: mouse@54

          // Can't rollback as-is the first deployment request: we need to know the default rollback version
          firstDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(firstDeploymentRequestId).map(_.get)
          rollbackSpecsForFirst <- engine.dbBinding.findExecutionSpecificationsForRollback(firstDeploymentRequest)

          // Can rollback
          thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
          rollbackSpecsForThird <- engine.dbBinding.findExecutionSpecificationsForRollback(thirdDeploymentRequest)

        } yield (
          rollbackSpecsForFirst,
          rollbackSpecsForThird.size,
          rollbackSpecsForThird("moon").get.id == secondExecSpecId,
          rollbackSpecsForThird("mars").get.id == firstExecSpecId,
          rollbackSpecsForThird("moon").get.version.toString,
          rollbackSpecsForThird("mars").get.version.toString
        ),
        2.seconds
      ) shouldBe(Map("moon" -> None, "mars" -> None), 2, true, true, """"54"""", """"27"""")
    }

    "check if an operation can be rolled back" in {
      Await.result(
        for {
          product <- engine.insertProduct("monkey")
          otherProduct <- engine.insertProduct("donkey")

          _ <- mockDeployExecution(product.name, "12", Map("orbit" -> Status.success))
          // Status = orbit: monkey@12

          (secondDeploymentRequestId, _) <- mockDeployExecution(product.name, "55", Map("orbit" -> Status.success, "venus" -> Status.success))
          // Status = orbit: monkey@55, venus: monkey@55

          (thirdDeploymentRequestId, _) <- mockDeployExecution(product.name, "69", Map("orbit" -> Status.success, "venus" -> Status.success))
          // Status = orbit: monkey@69, venus: monkey@69

          (otherDeploymentRequestId, _) <- mockDeployExecution(otherProduct.name, "215", Map("orbit" -> Status.success, "venus" -> Status.success))
          // Status = orbit: monkey@69 + bonobo@215, venus: monkey@69 + bonobo@215

          // Second request can't be rolled back
          secondDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
          rejectionOfSecond <- engine.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed
          // Third one can be rolled back, because it's the last one for its product
          thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
          _ <- engine.canRevertDeploymentRequest(thirdDeploymentRequest, isStarted = true)
          // Meanwhile the deployment on another product can be rolled back (even when it's the first one for that product: it just requires a default rollback version)
          otherDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(otherDeploymentRequestId).map(_.get)
          _ <- engine.canRevertDeploymentRequest(otherDeploymentRequest, isStarted = true)
        } yield rejectionOfSecond.getMessage,
        2.seconds
      ) shouldBe "a newer one has already been applied"
    }

    "perform a roll back" in {
      val defaultRollbackVersion = Version(""""00"""")
      Await.result(
        for {
          product <- engine.insertProduct("pony")

          (firstDeploymentRequestId, firstExecSpecId) <- mockDeployExecution(product.name, "11", Map("tic" -> Status.success, "tac" -> Status.success))
          // Status = tic: pony@11, tac: pony@11

          (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "22", Map("tic" -> Status.success, "tac" -> Status.success))
          secondDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
          // Status = tic: pony@22, tac: pony@22

          // Rollback the last deployment request
          rollbackOperationTraceIdA <- mockRollbackExecution(secondDeploymentRequestId, Map("tic" -> Status.success, "tac" -> Status.hostFailure), None)
          rollbackExecutionSpecIdsA <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(rollbackOperationTraceIdA)
          // Status = tic: pony@11, tac: pony@11

          (thirdDeploymentRequestId, thirdExecSpecId) <- mockDeployExecution(product.name, "33", Map("tic" -> Status.success, "tac" -> Status.success))
          // Status = tic: pony@33, tac: pony@33

          // Second request can't be rolled back anymore
          rejectionOfSecondA <- engine.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed

          // Rollback the last deployment request
          rollbackOperationTraceIdB <- mockRollbackExecution(thirdDeploymentRequestId, Map("tic" -> Status.success, "tac" -> Status.success), None)
          rollbackExecutionSpecIdsB <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(rollbackOperationTraceIdB)
          // Status = tic: pony@11, tac: pony@11

          // Second request still can't be rolled back
          rejectionOfSecondB <- engine.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed

          // Can rollback the first one now that the second one has been rolled back, but it requires to specify to which version to rollback
          detail <- engine.rollbackDeploymentRequest(firstDeploymentRequestId, "r.ollbacker", None).recover { case e: UnprocessableAction => e.detail }

          rollbackOperationTraceC <- engine.rollbackDeploymentRequest(firstDeploymentRequestId, "r.ollbacker", Some(defaultRollbackVersion)).map(_.get)
          rollbackExecutionSpecIdsC <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(rollbackOperationTraceC.id)
          rollbackExecutionSpecC <- engine.dbBinding.findExecutionSpecificationById(rollbackExecutionSpecIdsC.head).map(_.get)

        } yield (
          rollbackExecutionSpecIdsA.length,
          rollbackExecutionSpecIdsA.contains(firstExecSpecId),

          rejectionOfSecondA.getMessage,

          rollbackExecutionSpecIdsB.length,
          rollbackExecutionSpecIdsB.contains(firstExecSpecId),

          rejectionOfSecondB.getMessage,

          detail,

          rollbackOperationTraceC.deploymentRequestId == firstDeploymentRequestId,
          rollbackExecutionSpecIdsC.length,
          rollbackExecutionSpecC.version == defaultRollbackVersion
        ),
        2.seconds
      ) shouldBe(
        1, true,
        "a newer one has already been applied",
        1, true,
        "a newer one has already been applied",
        Map("required" -> "defaultVersion"),
        true, 1, true
      )
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
          _ <- engine.updateExecutionTrace(executionTrace.head.id, ExecutionState.completed, "", "", Map(
            "moon" -> TargetAtomStatus(Status.success, "no surprise"),
            "mars" -> TargetAtomStatus(Status.hostFailure, "no surprise")))
          retriedOperation <- engine.deployAgain(deploymentRequestId, "b.lightning").map(_.get)
          executionTracesAfterRetry <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId)
          _ <- engine.updateExecutionTrace(executionTracesAfterRetry.tail.head.id, ExecutionState.completed, "", "", Map(
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
