package com.criteo.perpetuo.engine

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.model._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class CrankshaftSpec extends SimpleScenarioTesting {
  test("A trivial execution triggers a job with no log href when there is no log href provided") {
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("product #1")
        depReq <- crankshaft.dbBinding.insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", "s.omeone"))
        _ <- crankshaft.startDeploymentRequest(depReq.id, "s.tarter")
        traces <- crankshaft.findExecutionTracesByDeploymentRequest(depReq.id)
      } yield traces.get.map(trace => (trace.id, trace.logHref)),
      1.second
    ) shouldEqual Seq((1, None))
  }

  test("Crankshaft keeps track of open executions for an operation") {
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("human")
        deploymentRequestId <- crankshaft.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "robert"))
        _ <- crankshaft.startDeploymentRequest(deploymentRequestId, "ignace")
        operationTraces <- dbBinding.findOperationTracesByDeploymentRequest(deploymentRequestId)
        operationTrace = operationTraces.head
        hasOpenExecutionBefore <- crankshaft.dbBinding.hasOpenExecutionTracesForOperation(operationTrace.id)
        _ <- closeOperation(operationTrace, Map("moon" -> Status.success, "mars" -> Status.hostFailure))
        hasOpenExecutionAfter <- crankshaft.dbBinding.hasOpenExecutionTracesForOperation(operationTrace.id)
        operationReClosingSucceeded <- crankshaft.dbBinding.closeOperationTrace(operationTrace)
      } yield (hasOpenExecutionBefore, hasOpenExecutionAfter, operationReClosingSucceeded.isDefined),
      2.seconds
    ) shouldBe(true, false, false)
  }

  def mockDeployExecution(productName: String, v: String, targetAtomToStatus: Map[String, Status.Code], initFailed: Boolean = false): Future[(Long, Long)] = {
    for {
      deploymentRequestId <- crankshaft.createDeploymentRequest(new DeploymentRequestAttrs(productName, Version(JsString(v).compactPrint), Seq(ProtoDeploymentPlanStep("", targetAtomToStatus.keys.toJson, "")), "", "r.equestor"))
      operationTrace <- crankshaft.startDeploymentRequest(deploymentRequestId, "s.tarter").map(_.get)
      executionSpecIds <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(operationTrace.id)
      _ <- closeOperation(operationTrace, targetAtomToStatus, initFailed)
    } yield (deploymentRequestId, executionSpecIds.head)
  }

  def mockRevertExecution(deploymentRequestId: Long, targetAtomToStatus: Map[String, Status.Code], defaultVersion: Option[Version] = None): Future[Long] = {
    for {
      operationTrace <- crankshaft.revert(deploymentRequestId, "r.everter", defaultVersion).map(_.get)
      _ <- closeOperation(operationTrace, targetAtomToStatus)
    } yield operationTrace.id
  }

  test("Crankshaft checks that an operation can be started only if previous transactions on the same product have been completed") {
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("pig")

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
        _ <- mockDeployExecution(product.name, "102", Map("corn-field" -> Status.notDone), initFailed = true)

        // OK after a init failure
        _ <- mockDeployExecution(product.name, "103", Map("racing" -> Status.success))
      } yield {
        conflictMsg shouldBe "Cannot be processed for the moment because a conflicting transaction is ongoing, which must first succeed or be reverted"
      },
      2.seconds
    )
  }

  test("Crankshaft checks if an operation can be retried") {
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("horse")

        (firstDeploymentRequestId, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.success))
        // Status = corn-field: horse@100

        (secondDeploymentRequestId, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success, "pool" -> Status.success))
        // Status = corn-field: horse@100, racing: horse@101, pool: horse@101

        (thirdDeploymentRequestId, _) <- mockDeployExecution(product.name, "102", Map("racing" -> Status.productFailure))
        // Status = corn-field: horse@100, racing: horse@102?, pool: horse@101

        // fixme: one day, first deployment will be retryable:
        // But second one can't be, because it impacts `racing`, whose status changed in the meantime
        secondDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
        rejectionOfSecond <- crankshaft.canDeployDeploymentRequest(secondDeploymentRequest).failed
        // The last one of course is retryable
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
        _ <- crankshaft.canDeployDeploymentRequest(thirdDeploymentRequest)
      } yield rejectionOfSecond.getMessage.split(":")(1).trim,
      2.seconds
    ) shouldBe "a newer one has already been applied"
  }

  test("Crankshaft finds executions for reverting") {
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("mouse")

        (firstDeploymentRequestId, firstExecSpecId) <- mockDeployExecution(product.name, "27", Map("moon" -> Status.success, "mars" -> Status.success))
        // Status = moon: mouse@27, mars: mouse@27

        (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "54", Map("moon" -> Status.success, "venus" -> Status.success))
        // Status = moon: mouse@54, mars: mouse@27, venus: mouse@54

        (thirdDeploymentRequestId, thirdExecSpecId) <- mockDeployExecution(product.name, "69", Map("moon" -> Status.success, "mars" -> Status.productFailure))
        // Status = moon: mouse@69, mars: mouse@69?, venus: mouse@54

        // Can't revert as-is the first deployment request: we need to know the default revert version
        firstDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(firstDeploymentRequestId).map(_.get)
        (undeterminedSpecsFirst, determinedSpecsFirst) <- crankshaft.dbBinding.findExecutionSpecificationsForRevert(firstDeploymentRequest)

        // Can revert
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
        (undeterminedSpecsThird, determinedSpecsThird) <- crankshaft.dbBinding.findExecutionSpecificationsForRevert(thirdDeploymentRequest)

      } yield {
        val specsThird = determinedSpecsThird.map { case (spec, targets) => spec.id -> (spec.version.value, targets) }.toMap
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

  test("Crankshaft checks if an operation can be reverted") {
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("monkey")
        otherProduct <- crankshaft.insertProductIfNotExists("donkey")

        _ <- mockDeployExecution(product.name, "12", Map("orbit" -> Status.success))
        // Status = orbit: monkey@12

        (secondDeploymentRequestId, _) <- mockDeployExecution(product.name, "55", Map("orbit" -> Status.success, "venus" -> Status.success))
        // Status = orbit: monkey@55, venus: monkey@55

        (thirdDeploymentRequestId, _) <- mockDeployExecution(product.name, "69", Map("orbit" -> Status.success, "venus" -> Status.success))
        // Status = orbit: monkey@69, venus: monkey@69

        (otherDeploymentRequestId, _) <- mockDeployExecution(otherProduct.name, "215", Map("orbit" -> Status.success, "venus" -> Status.success))
        // Status = orbit: monkey@69 + bonobo@215, venus: monkey@69 + bonobo@215

        // Second request can't be reverted
        secondDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
        rejectionOfSecond <- crankshaft.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed
        // Third one can be reverted, because it's the last one for its product
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(thirdDeploymentRequestId).map(_.get)
        _ <- crankshaft.canRevertDeploymentRequest(thirdDeploymentRequest, isStarted = true)
        // Meanwhile the deployment on another product can be reverted (even when it's the first one for that product: it just requires a default revert version)
        otherDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(otherDeploymentRequestId).map(_.get)
        _ <- crankshaft.canRevertDeploymentRequest(otherDeploymentRequest, isStarted = true)
      } yield rejectionOfSecond.getMessage.split(":")(1).trim,
      2.seconds
    ) shouldBe "a newer one has already been applied"
  }

  test("Crankshaft performs a revert") {
    val defaultRevertVersion = Version(""""00"""")
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("pony")

        (firstDeploymentRequestId, firstExecSpecId) <- mockDeployExecution(product.name, "11", Map("tic" -> Status.success, "tac" -> Status.success))
        // Status = tic: pony@11, tac: pony@11

        (secondDeploymentRequestId, secondExecSpecId) <- mockDeployExecution(product.name, "22", Map("tic" -> Status.success, "tac" -> Status.success))
        secondDeploymentRequest <- crankshaft.dbBinding.findDeepDeploymentRequestById(secondDeploymentRequestId).map(_.get)
        // Status = tic: pony@22, tac: pony@22

        // Revert the last deployment request
        revertOperationTraceIdA <- mockRevertExecution(secondDeploymentRequestId, Map("tic" -> Status.success, "tac" -> Status.hostFailure), None)
        revertExecutionSpecIdsA <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceIdA)
        // Status = tic: pony@11, tac: pony@11

        (thirdDeploymentRequestId, thirdExecSpecId) <- mockDeployExecution(product.name, "33", Map("tic" -> Status.success, "tac" -> Status.success))
        // Status = tic: pony@33, tac: pony@33

        // Second request can't be reverted anymore
        rejectionOfSecondA <- crankshaft.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed

        // Revert the last deployment request
        revertOperationTraceIdB <- mockRevertExecution(thirdDeploymentRequestId, Map("tic" -> Status.success, "tac" -> Status.success), None)
        revertExecutionSpecIdsB <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceIdB)
        // Status = tic: pony@11, tac: pony@11

        // Second request still can't be reverted
        rejectionOfSecondB <- crankshaft.canRevertDeploymentRequest(secondDeploymentRequest, isStarted = true).failed

        // Can revert the first one now that the second one has been reverted, but it requires to specify to which version to revert
        required <- crankshaft.revert(firstDeploymentRequestId, "r.ollbacker", None).recover { case MissingInfo(_, required) => required }

        revertOperationTraceC <- crankshaft.revert(firstDeploymentRequestId, "r.ollbacker", Some(defaultRevertVersion)).map(_.get)
        revertExecutionSpecIdsC <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceC.id)
        revertExecutionSpecC <- crankshaft.dbBinding.findExecutionSpecificationById(revertExecutionSpecIdsC.head).map(_.get)

      } yield (
        revertExecutionSpecIdsA.length,
        revertExecutionSpecIdsA.contains(firstExecSpecId),

        rejectionOfSecondA.getMessage.split(":")(1).trim,

        revertExecutionSpecIdsB.length,
        revertExecutionSpecIdsB.contains(firstExecSpecId),

        rejectionOfSecondB.getMessage.split(":")(1).trim,

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

  test("Crankshaft keeps track of retried operation") {
    Await.result(
      for {
        product <- crankshaft.insertProductIfNotExists("martian")
        deploymentRequestId <- crankshaft.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "robert"))
        _ <- crankshaft.startDeploymentRequest(deploymentRequestId, "ignace")
        operationTraces <- dbBinding.findOperationTracesByDeploymentRequest(deploymentRequestId)
        operationTrace = operationTraces.head
        firstExecutionTraces <- closeOperation(operationTrace, Map("moon" -> Status.success, "mars" -> Status.hostFailure))
        retriedOperation <- crankshaft.deployAgain(deploymentRequestId, "b.lightning").map(_.get)
        secondExecutionTraces <- closeOperation(retriedOperation, Map("moon" -> Status.success, "mars" -> Status.success))
        hasOpenExecutionAfter <- crankshaft.dbBinding.hasOpenExecutionTracesForOperation(retriedOperation.id)
        operationReClosingSucceeded <- crankshaft.dbBinding.closeOperationTrace(retriedOperation)
        initialExecutionSpecIds <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(operationTrace.id)
        retriedExecutionSpecIds <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(retriedOperation.id)
      } yield (
        firstExecutionTraces.length,
        secondExecutionTraces.length,
        firstExecutionTraces.intersect(secondExecutionTraces).length,
        retriedOperation.id == operationTrace.id,
        hasOpenExecutionAfter, operationReClosingSucceeded.isDefined,
        initialExecutionSpecIds.length == retriedExecutionSpecIds.length,
        initialExecutionSpecIds == retriedExecutionSpecIds
      ),
      2.seconds
    ) shouldBe(1, 1, 0, false, false, false, true, true)
  }

  test("Crankshaft's binding provides the last version deployed on a given target") {
    def v(versionName: String): Version = Version(JsString(versionName))

    deploy("mournful-moray", "v13", Seq("paris", "london", "tokyo"))
    deploy("pirate-piranha", "0.0.1", Seq("london", "tokyo"))
    deploy("mournful-moray", "v14.doomed", Seq("paris", "tokyo"), Status.notDone)
    deploy("mournful-moray", "v13.eu", Seq("paris", "london", "kuala lumpur"), Status.productFailure)

    // projects don't override each other
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget("pirate-piranha", Seq("paris", "london", "tokyo")) should
      become(Map("london" -> v("0.0.1"), "tokyo" -> v("0.0.1"))) // no "paris"

    // if a version failed to start, it still counts as the last deployed version
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget("mournful-moray", Seq("london", "paris", "new-york")) should
      become(Map("paris" -> v("v13.eu"), "london" -> v("v13.eu"))) // no "new-york"

    // if a version has not been actually deployed on a target (i.e. despite the request, see status `notDone`)
    // it must not be considered as the last deployed version on the target
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget("mournful-moray", Seq("tokyo")) should
      become(Map("tokyo" -> v("v13")))

    revert("mournful-moray", Some("prewar"))
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget("mournful-moray", Seq("paris", "london", "tokyo", "kuala lumpur")) should
      become(Map("paris" -> v("v13"), "london" -> v("v13"), "tokyo" -> v("v13"), "kuala lumpur" -> v("prewar")))
  }

  test("Crankshaft computes the dominant version when given an ordered sequence of reference pools") {
    def v(versionName: String): Option[Version] = Some(Version(JsString(versionName)))

    deploy("spatial-sparrow", "hot-fix", Seq("sun"))
    deploy("side-siberian", "universal", Seq("sun", "earth", "saturn", "proxima"))
    deploy("spatial-sparrow", "sunny", Seq("mercury", "venus", "earth", "mars"))
    deploy("spatial-sparrow", "cold", Seq("jupiter", "saturn", "uranus", "neptune"))
    deploy("side-siberian", "future", Seq("earth", "mars", "neptune"))
    deploy("spatial-sparrow", "fantasy", Seq("earth", "mars", "moon"))
    revert("spatial-sparrow", defaultVersion = Some("big-bang"))
    deploy("spatial-sparrow", "no-op", Seq("mercury", "earth"), Status.notDone)

    crankshaft.computeDominantVersion("spatial-sparrow", Seq(
      Seq("unknown"))
    ) should become(Option.empty[Version])

    crankshaft.computeDominantVersion("spatial-sparrow", Seq(
      Seq("sun"))
    ) should become(v("hot-fix"))

    crankshaft.computeDominantVersion("spatial-sparrow", Seq(
      Seq("sun", "mercury", "venus"), // hot-fix, sunny, sunny
      Seq("jupiter", "saturn", "uranus", "neptune") // cold, cold, cold, cold
    )) should become(v("sunny"))

    crankshaft.computeDominantVersion("spatial-sparrow", Seq(
      Seq("unknown", "target"), //
      Seq("mercury", "venus"), // sunny, sunny
      Seq("jupiter", "saturn", "uranus", "neptune") // cold, cold, cold, cold
    )) should become(v("sunny"))

    crankshaft.computeDominantVersion("spatial-sparrow", Seq(
      Seq("moon")
    )) should become(v("big-bang"))

    crankshaft.computeDominantVersion("spatial-sparrow", Seq(
      Seq("earth", "mars", "uranus"), // sunny (from revert), sunny (from revert), cold
      Seq("jupiter", "saturn", "neptune") // cold, cold, cold
    )) should become(v("sunny"))
  }
}


class CrankshaftWithFailingExecutorSpec extends SimpleScenarioTesting {
  override protected def triggerMock = throw new RuntimeException("too bad, dude")

  test("Crankshaft keeps the created records in DB and marks an execution trace as failed if the trigger fails") {
    val res = for {
      product <- crankshaft.insertProductIfNotExists("airplane")
      deploymentRequestId <- crankshaft.createDeploymentRequest(new DeploymentRequestAttrs(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "bob"))
      _ <- crankshaft.startDeploymentRequest(deploymentRequestId, "ignace")
      operationTraces <- dbBinding.findOperationTracesByDeploymentRequest(deploymentRequestId)
      operationTrace = operationTraces.head
      hasOpenExecution <- crankshaft.dbBinding.hasOpenExecutionTracesForOperation(operationTrace.id)
      executionTrace <- crankshaft.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).map(_.head)
    } yield (
      hasOpenExecution, executionTrace.state
    )
    res should become(false, ExecutionState.initFailed)
  }
}


class CrankshaftWithNoLogHrefSpec extends SimpleScenarioTesting {
  test("Cannot stop an execution that has no log href") {
    val op = deploy("dusty-duck", "12345", Seq("thailand"))
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(execTrace => crankshaft.stopExecution(execTrace, "joe")) should
      asynchronouslyThrow[RuntimeException]("No log href for execution trace .+")
  }
}


class CrankshaftWithUnknownLogHrefSpec extends SimpleScenarioTesting {
  private val logHref = "now you can track me down"

  override protected def triggerMock = Some(logHref)

  test("A trivial execution triggers a job with a log href when a log href is provided") {
    val op = deploy("product #2", "1000", Seq("*"))
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.flatMap(_.logHref).toSet) should eventually(be(Set(logHref)))
  }

  test("Cannot stop an execution of an unknown type") {
    val op = deploy("dusty-duck", "123456", Seq("thailand"))
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(crankshaft.stopExecution(_, "joe")) should
      asynchronouslyThrow[RuntimeException]("Could not find an execution configuration for the type `unknown`")
  }
}


class CrankshaftWithRundeckLogHrefSpec extends SimpleScenarioTesting {
  private val logHref = "https://executor.tld/execution/show/42"

  override protected def triggerMock = Some(logHref)

  test("Cannot stop an execution of an explicitly unstoppable type") {
    val op = deploy("dusty-duck", "1234567", Seq("thailand"))
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(crankshaft.stopExecution(_, "joe")) should
      asynchronouslyThrow[RuntimeException](s"This kind of execution cannot be stopped: $logHref")
  }

  test("Crankshaft tries to stop executions, which might terminate normally at the same time") {
    deploy("dusty-duck", "1", Seq("here"))
    val lastOp = deploy("dusty-duck", "2", Seq("here", "there"))
    val req = lastOp.deploymentRequest

    // try to stop when everything is already terminated
    crankshaft.tryStopDeploymentRequest(req, "killer-guy") should
      eventually(be((0, Seq())))

    // try to stop when nothing has been terminated but it's impossible to stop
    crankshaft.revert(req.id, "r.everter", Some(Version("0".toJson))).map(_.get).flatMap(op =>
      crankshaft.tryStopDeploymentRequest(req, "killer-guy")
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.length)
          )
        }
    ) should eventually(be((2, 2, 0, 2))) // i.e. 2 execution traces, 2 closed, 0 stopped, 2 failures

    // try to stop when one execution is already terminated and the other one could not be stopped (so 0 success)
    crankshaft.revert(req.id, "r.everter", Some(Version("0".toJson))).map(_.get).flatMap(op =>
      crankshaft.dbBinding.findExecutionIdsByOperationTrace(op.id)
        .flatMap { executionIds =>
          val executionId = executionIds.head // only update the first execution (out of the 2 triggered by the revert)
          crankshaft.dbBinding.findTargetsByExecution(executionId).flatMap(atoms =>
            crankshaft.dbBinding.findExecutionTraceIdsByExecution(executionId).flatMap(executionTraceIds =>
              Future.traverse(executionTraceIds)(executionTraceId =>
                crankshaft.updateExecutionTrace(
                  executionTraceId, ExecutionState.aborted, "from the executor", None,
                  atoms.map(_ -> TargetAtomStatus(Status.success, "")).toMap
                ))
            ))
        }
        .flatMap(_ => crankshaft.tryStopDeploymentRequest(req, "killer-guy"))
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.map(_.split(":").head))
          )
        }
    ) should eventually(be((2, 1, 0, Seq("This kind of execution cannot be stopped")))) // i.e. 2 execution traces, 1 closed, 0 stopped, 1 failure
  }
}
