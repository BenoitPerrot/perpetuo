package com.criteo.perpetuo.engine

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.config.{PluginLoader, Plugins}
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, UnprocessableIntent}
import com.criteo.perpetuo.engine.invokers.DummyInvoker
import com.criteo.perpetuo.model._
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


object FailingInvoker extends DummyInvoker("DummyWithLogHref") {
  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    throw new RuntimeException("too bad, dude")
  }
}

object DummyInvokerWithLogHref extends DummyInvoker("DummyWithLogHref") {
  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    super.trigger(execTraceId, productName, version, target, initiator).map { logHref =>
      assert(logHref.isEmpty)
      Some("now you can track me down")
    }
  }
}


class EngineSpec extends Test with TestDb {

  private val plugins = new Plugins(new PluginLoader)
  // TODO: should instantiate TargetDispatcherForTesting explicitly instead of by-conf, for clarity
  private val engine = new Engine(new DbBinding(dbContext), plugins.resolver, plugins.dispatcher, plugins.permissions, plugins.listeners)

  private val futureProductWithNoDeployType = engine.insertProduct(TargetDispatcherForTesting.productWithNoDeployTypeName)


  test("A trivial execution triggers a job with no log href when there is no log href provided") {
    Await.result(
      for {
        product <- engine.insertProduct("product #1")
        depReq <- engine.dbBinding.insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"1000\""), """"*"""", "", "s.omeone", new Timestamp(123456789)))
        _ <- engine.startDeploymentRequest(depReq.id, "s.tarter")
        traces <- engine.findExecutionTracesByDeploymentRequest(depReq.id)
      } yield traces.get.map(trace => (trace.id, trace.logHref)),
      1.second
    ) shouldEqual Seq((1, None))
  }

  test("A trivial execution triggers a job with a log href when a log href is provided as a Future") {
    val eng = new Engine(new DbBinding(dbContext), plugins.resolver, SingleTargetDispatcher(DummyInvokerWithLogHref), plugins.permissions, plugins.listeners)
    Await.result(
      for {
        product <- engine.insertProduct("product #2")
        depReq <- eng.dbBinding.insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"1000\""), """"*"""", "", "s.omeone", new Timestamp(123456789)))
        _ <- eng.startDeploymentRequest(depReq.id, "s.tarter")
        traces <- eng.findExecutionTracesByDeploymentRequest(depReq.id)
      } yield traces.get.map(trace => (trace.id, trace.logHref)),
      1.second
    ) shouldEqual Seq((2, Some("now you can track me down")))
  }

  test("Engine keeps track of open executions for an operation") {
    Await.result(
      for {
        product <- engine.insertProduct("human")
        deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("42").compactPrint), """["moon","mars"]}""", "", "robert", new Timestamp(System.currentTimeMillis))).map(_ ("id").toString.toLong)
        _ <- engine.startDeploymentRequest(deploymentRequestId, "ignace")
        operationTraces <- engine.findOperationTracesByDeploymentRequest(deploymentRequestId).map(_.get)
        operationTrace = operationTraces.head
        hasOpenExecutionBefore <- engine.dbBinding.hasOpenExecutionTracesForOperation(operationTrace.id)
        executionTrace <- engine.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).map(_.head)
        _ <- engine.updateExecutionTrace(executionTrace.id, ExecutionState.completed, "", "", Map(
          "moon" -> TargetAtomStatus(Status.success, "no surprise"),
          "mars" -> TargetAtomStatus(Status.hostFailure, "no surprise")))
        hasOpenExecutionAfter <- engine.dbBinding.hasOpenExecutionTracesForOperation(operationTrace.id)
        operationReClosingSucceeded <- engine.dbBinding.closeOperationTrace(operationTrace)
      } yield (
        hasOpenExecutionBefore, hasOpenExecutionAfter, operationReClosingSucceeded.isDefined
      ),
      2.seconds
    ) shouldBe(true, false, false)
  }

  test("Engine rejects deployment request for products with no deploy type") {
    Await.result(
      futureProductWithNoDeployType
        .flatMap(product =>
          try {
            engine.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("1").compactPrint), """["sink"]""", "", "c.reator", new Timestamp(System.currentTimeMillis)))
              .map(_ => false)
          } catch {
            case _: UnprocessableIntent => Future.successful(true)
          }
        ),
      2.seconds
    ) shouldBe true
  }

  def mockDeployExecution(productName: String, v: String, targetAtomToStatus: Map[String, Status.Code], initFailure: Option[String] = None): Future[(Long, Long)] = {
    for {
      deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(productName, Version(JsString(v).compactPrint), targetAtomToStatus.keys.toJson.compactPrint, "", "r.equestor", new Timestamp(System.currentTimeMillis))).map(_ ("id").toString.toLong)
      operationTraceId <- engine.startDeploymentRequest(deploymentRequestId, "s.tarter").map(_.get.id)
      executionTraceIds <- engine.dbBinding.findExecutionTraceIdsByOperationTrace(operationTraceId)
      executionSpecIds <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(operationTraceId)
      _ <- engine.updateExecutionTrace(executionTraceIds.head, initFailure.map(_ => ExecutionState.initFailed).getOrElse(ExecutionState.completed), initFailure.getOrElse(""), "", targetAtomToStatus.mapValues(c => TargetAtomStatus(c, "")))
    } yield (deploymentRequestId, executionSpecIds.head)
  }

  def mockRevertExecution(deploymentRequestId: Long, targetAtomToStatus: Map[String, Status.Code], defaultVersion: Option[Version] = None): Future[Long] = {
    for {
      operationTraceId <- engine.revert(deploymentRequestId, "r.everter", defaultVersion).map(_.get.id)
      executionTraceIds <- engine.dbBinding.findExecutionTraceIdsByOperationTrace(operationTraceId)
      _ <- Future.traverse(executionTraceIds)(
        engine.updateExecutionTrace(_, ExecutionState.completed, "", "", targetAtomToStatus.mapValues(c => TargetAtomStatus(c, "")))
      )
    } yield operationTraceId
  }

  test("Engine checks that an operation can be started only if previous transactions on the same product have been completed") {
    Await.result(
      for {
        product <- engine.insertProduct("pig")

        // OK if it's the first
        _ <- mockDeployExecution(product.name, "99", Map("racing" -> Status.success))

        // OK after a success
        (secondId, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.hostFailure))

        // not OK if it's after a deployment failure
        conflictMsg <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))
          .map(_ => "unrejected").recover { case Conflict(msg, _) => msg }

        // the failing one must be reverted first
        _ <- mockRevertExecution(secondId, Map("corn-field" -> Status.hostFailure), Some(Version(""""big-bang"""")))

        // OK after the failing has been reverted (even if the revert failed)
        (thirdId, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))

        // note that we can revert a successful operation
        _ <- mockRevertExecution(thirdId, Map("racing" -> Status.success))

        // OK after a revert of a successful operation
        _ <- mockDeployExecution(product.name, "102", Map("corn-field" -> Status.notDone), Some("crashed at start"))

        // OK after a init failure
        _ <- mockDeployExecution(product.name, "103", Map("racing" -> Status.success))
      } yield {
        conflictMsg shouldBe "Cannot be processed for the moment because a conflicting transaction is ongoing, which must first succeed or be reverted"
      },
      2.seconds
    )
  }

  test("Engine checks if an operation can be retried") {
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

  test("Engine finds executions for reverting") {
    Await.result(
      for {
        product <- engine.insertProduct("mouse")

        (firstDeploymentRequestId, firstExecSpecId) <- mockDeployExecution(product.name, "27", Map("moon" -> Status.success, "mars" -> Status.success))
        // Status = moon: mouse@27, mars: mouse@27

        (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "54", Map("moon" -> Status.success, "venus" -> Status.success))
        // Status = moon: mouse@54, mars: mouse@27, venus: mouse@54

        (thirdDeploymentRequestId, thirdExecSpecId) <- mockDeployExecution(product.name, "69", Map("moon" -> Status.success, "mars" -> Status.productFailure))
        // Status = moon: mouse@69, mars: mouse@69?, venus: mouse@54

        // Can't revert as-is the first deployment request: we need to know the default revert version
        firstDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(firstDeploymentRequestId).map(_.get)
        (undeterminedSpecsFirst, determinedSpecsFirst) <- engine.dbBinding.findExecutionSpecificationsForRevert(firstDeploymentRequest)

        // Can revert
        thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
        (undeterminedSpecsThird, determinedSpecsThird) <- engine.dbBinding.findExecutionSpecificationsForRevert(thirdDeploymentRequest)

      } yield {
        val specsThird = determinedSpecsThird.map { case (spec, targets) => spec.id -> (spec.version.toString, targets) }.toMap
        (
          undeterminedSpecsFirst,
          determinedSpecsFirst.isEmpty,
          undeterminedSpecsThird.isEmpty,
          specsThird(firstExecSpecId),
          specsThird(secondExecSpecId)
        )
      },
      2.seconds
    ) shouldBe(Set("moon", "mars"), true, true, (""""27"""", Set("mars")), (""""54"""", Set("moon")))
  }

  test("Engine checks if an operation can be reverted") {
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

        // Second request can't be reverted
        secondDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
        rejectionOfSecond <- engine.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed
        // Third one can be reverted, because it's the last one for its product
        thirdDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
        _ <- engine.canRevertDeploymentRequest(thirdDeploymentRequest, isStarted = true)
        // Meanwhile the deployment on another product can be reverted (even when it's the first one for that product: it just requires a default revert version)
        otherDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(otherDeploymentRequestId).map(_.get)
        _ <- engine.canRevertDeploymentRequest(otherDeploymentRequest, isStarted = true)
      } yield rejectionOfSecond.getMessage,
      2.seconds
    ) shouldBe "a newer one has already been applied"
  }

  test("Engine performs a revert") {
    val defaultRevertVersion = Version(""""00"""")
    Await.result(
      for {
        product <- engine.insertProduct("pony")

        (firstDeploymentRequestId, firstExecSpecId) <- mockDeployExecution(product.name, "11", Map("tic" -> Status.success, "tac" -> Status.success))
        // Status = tic: pony@11, tac: pony@11

        (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "22", Map("tic" -> Status.success, "tac" -> Status.success))
        secondDeploymentRequest <- engine.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
        // Status = tic: pony@22, tac: pony@22

        // Revert the last deployment request
        revertOperationTraceIdA <- mockRevertExecution(secondDeploymentRequestId, Map("tic" -> Status.success, "tac" -> Status.hostFailure), None)
        revertExecutionSpecIdsA <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceIdA)
        // Status = tic: pony@11, tac: pony@11

        (thirdDeploymentRequestId, thirdExecSpecId) <- mockDeployExecution(product.name, "33", Map("tic" -> Status.success, "tac" -> Status.success))
        // Status = tic: pony@33, tac: pony@33

        // Second request can't be reverted anymore
        rejectionOfSecondA <- engine.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed

        // Revert the last deployment request
        revertOperationTraceIdB <- mockRevertExecution(thirdDeploymentRequestId, Map("tic" -> Status.success, "tac" -> Status.success), None)
        revertExecutionSpecIdsB <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceIdB)
        // Status = tic: pony@11, tac: pony@11

        // Second request still can't be reverted
        rejectionOfSecondB <- engine.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed

        // Can revert the first one now that the second one has been reverted, but it requires to specify to which version to revert
        required <- engine.revert(firstDeploymentRequestId, "r.ollbacker", None).recover { case MissingInfo(_, required) => required }

        revertOperationTraceC <- engine.revert(firstDeploymentRequestId, "r.ollbacker", Some(defaultRevertVersion)).map(_.get)
        revertExecutionSpecIdsC <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceC.id)
        revertExecutionSpecC <- engine.dbBinding.findExecutionSpecificationById(revertExecutionSpecIdsC.head).map(_.get)

      } yield (
        revertExecutionSpecIdsA.length,
        revertExecutionSpecIdsA.contains(firstExecSpecId),

        rejectionOfSecondA.getMessage,

        revertExecutionSpecIdsB.length,
        revertExecutionSpecIdsB.contains(firstExecSpecId),

        rejectionOfSecondB.getMessage,

        required,

        revertOperationTraceC.deploymentRequestId == firstDeploymentRequestId,
        revertExecutionSpecIdsC.length,
        revertExecutionSpecC.version == defaultRevertVersion
      ),
      2.seconds
    ) shouldBe(
      1, true,
      "a newer one has already been applied",
      1, true,
      "a newer one has already been applied",
      "defaultVersion",
      true, 1, true
    )
  }

  test("Engine keeps track of retried operation") {
    Await.result(
      for {
        product <- engine.insertProduct("martian")
        deploymentRequestId <- engine.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("42").compactPrint), """["moon","mars"]}""", "", "robert", new Timestamp(System.currentTimeMillis))).map(_ ("id").toString.toLong)
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
        operationReClosingSucceeded <- engine.dbBinding.closeOperationTrace(retriedOperation)
        initialExecutionSpecIds <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(operationTraceId)
        retriedExecutionSpecIds <- engine.dbBinding.findExecutionSpecIdsByOperationTrace(retriedOperation.id)
      } yield (
        executionTrace.length,
        executionTracesAfterRetry.length,
        retriedOperation.id == operationTraceId,
        hasOpenExecutionAfter, operationReClosingSucceeded.isDefined,
        initialExecutionSpecIds.length == retriedExecutionSpecIds.length,
        initialExecutionSpecIds == retriedExecutionSpecIds
      ),
      2.seconds
    ) shouldBe(1, 2, false, false, false, true, true)
  }

}
