package com.criteo.perpetuo.engine

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.engine.executors._
import com.criteo.perpetuo.model._
import org.mockito.Mockito.when
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class CrankshaftSpec extends SimpleScenarioTesting {

  private def hasOpenExecutionTracesForOperation(operationTraceId: Long) =
    dbContext.db.run(crankshaft.dbBinding.hasOpenExecutionTracesForOperation(operationTraceId))

  private def gettingPlanStepToOperateAndLastDoneStep(deploymentRequest: DeploymentRequest, operation: Operation.Kind) =
    dbContext.db.run(crankshaft.fuelFilter.gettingPlanStepToOperateAndLastDoneStep(deploymentRequest, operation))

  private def closeOperationTrace(operationTrace: OperationTrace): Future[Option[OperationTrace]] =
    dbContext.db.run(crankshaft.dbBinding.closingOperationTrace(operationTrace))

  private def rejectIfCannot(operationKind: Operation.Kind, deploymentRequest: DeploymentRequest) =
    dbContext.db.run(operationKind match {
      case Operation.deploy => crankshaft.fuelFilter.rejectingIfCannotDeploy(deploymentRequest)
      case Operation.revert => crankshaft.fuelFilter.rejectingIfCannotRevert(deploymentRequest)
    })

  test("A trivial execution triggers a job with no log href when there is no log href provided") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("product #1")
        depPlan <- crankshaft.dbBinding.insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", "s.omeone"))
        (toDoBeforeStart, lastDoneBeforeStart) <- gettingPlanStepToOperateAndLastDoneStep(depPlan.deploymentRequest, Operation.deploy)
        _ <- crankshaft.step(depPlan.deploymentRequest, Some(0), "s.tarter")
        (toDoAfterStart, lastDoneAfterStart) <- gettingPlanStepToOperateAndLastDoneStep(depPlan.deploymentRequest, Operation.deploy)
        traces <- crankshaft.findExecutionTracesByDeploymentRequest(depPlan.deploymentRequest.id)
      } yield (
        traces.get.map(trace => (trace.id, trace.logHref)),
        lastDoneBeforeStart.isEmpty,
        lastDoneAfterStart.contains(toDoBeforeStart),
        toDoBeforeStart == toDoAfterStart // Deployment flopped, so the next step remains the same
      )
    ) shouldEqual(
      Seq((1, None)),
      true, true, true
    )
  }

  test("Crankshaft keeps track of open executions for an operation") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("human")
        deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "robert"))
        operationTrace <- crankshaft.step(deploymentRequest, Some(0), "ignace")
        hasOpenExecutionBefore <- hasOpenExecutionTracesForOperation(operationTrace.id)
        _ <- closeOperation(operationTrace, Map("moon" -> Status.success, "mars" -> Status.hostFailure))
        hasOpenExecutionAfter <- hasOpenExecutionTracesForOperation(operationTrace.id)
        operationReClosingSucceeded <- closeOperationTrace(operationTrace)
      } yield (hasOpenExecutionBefore, hasOpenExecutionAfter, operationReClosingSucceeded.isDefined)
    ) shouldBe(true, false, false)
  }

  def mockDeployExecution(productName: String, v: String, targetAtomToStatus: Map[String, Status.Code], initFailed: Boolean = false, updateTargetStatuses: Boolean = true): Future[(DeploymentRequest, Long)] = {
    for {
      deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(productName, Version(JsString(v).compactPrint), Seq(ProtoDeploymentPlanStep("", targetAtomToStatus.keys.toJson, "")), "", "r.equestor"))
      operationTrace <- crankshaft.step(deploymentRequest, Some(0), "s.tarter")
      executionSpecIds <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(operationTrace.id)
      _ <- closeOperation(operationTrace, if (updateTargetStatuses) targetAtomToStatus else Map(), initFailed)
    } yield (deploymentRequest, executionSpecIds.head)
  }

  def mockRevertExecution(deploymentRequest: DeploymentRequest, targetAtomToStatus: Map[String, Status.Code], defaultVersion: Option[Version] = None): Future[Long] = {
    for {
      operationTrace <- crankshaft.revert(deploymentRequest, None, "r.everter", defaultVersion)
      _ <- closeOperation(operationTrace, targetAtomToStatus)
    } yield operationTrace.id
  }

  test("Crankshaft checks that an operation can be started only if previous transactions on the same product have been completed") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("pig")

        // OK if it's the first
        _ <- mockDeployExecution(product.name, "99", Map("racing" -> Status.success))

        // OK after a success
        (second, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.hostFailure))

        // not OK if it's after a deployment failure
        conflictMsg <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))
          .map(_ => "unrejected").recover { case Conflict(msg, _) => msg }

        // the failing one must be reverted first
        _ <- mockRevertExecution(second, Map("corn-field" -> Status.hostFailure), Some(Version(JsString("big-bang"))))

        // OK after the failing has been reverted (even if the revert failed)
        (third, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))

        // note that we can revert a successful operation
        _ <- mockRevertExecution(third, Map("racing" -> Status.success))

        // OK after a revert of a successful operation
        _ <- mockDeployExecution(product.name, "102", Map("corn-field" -> Status.notDone), initFailed = true)

        // OK after a init failure
        _ <- mockDeployExecution(product.name, "103", Map("racing" -> Status.success))
      } yield {
        conflictMsg shouldBe "Cannot be processed for the moment because a conflicting transaction is ongoing, which must first succeed or be reverted"
      }
    )
  }

  test("Crankshaft checks if an operation can be retried") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("horse")

        (firstDeploymentRequest, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.success))
        // Status = corn-field: horse@100

        (secondDeploymentRequest, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success, "pool" -> Status.success))
        // Status = corn-field: horse@100, racing: horse@101, pool: horse@101

        (thirdDeploymentRequest, _) <- mockDeployExecution(product.name, "102", Map("racing" -> Status.productFailure))
        // Status = corn-field: horse@100, racing: horse@102?, pool: horse@101

        // fixme: one day, first deployment will be retryable:
        // But second one can't be, because it impacts `racing`, whose status changed in the meantime
        secondDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(secondDeploymentRequest.id).map(_.get)
        rejectionOfSecond <- rejectIfCannot(Operation.deploy, secondDeploymentRequest).failed
        // The last one of course is retryable
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(thirdDeploymentRequest.id).map(_.get)
        _ <- rejectIfCannot(Operation.deploy, thirdDeploymentRequest)
      } yield rejectionOfSecond.getMessage.split(":")(1).trim
    ) shouldBe "a newer one has already been applied"
  }

  test("Crankshaft finds executions for reverting") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("mouse")

        (firstDeploymentRequest, firstExecSpecId) <- mockDeployExecution(product.name, "27", Map("moon" -> Status.success, "mars" -> Status.success))
        // Status = moon: mouse@27, mars: mouse@27

        (secondDeploymentRequest, secondExecSpecId) <- mockDeployExecution(product.name, "54", Map("moon" -> Status.success, "venus" -> Status.success))
        // Status = moon: mouse@54, mars: mouse@27, venus: mouse@54

        (thirdDeploymentRequest, thirdExecSpecId) <- mockDeployExecution(product.name, "69", Map("moon" -> Status.success, "mars" -> Status.productFailure))
        // Status = moon: mouse@69, mars: mouse@69?, venus: mouse@54

        // Can't revert as-is the first deployment request: we need to know the default revert version
        firstDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(firstDeploymentRequest.id).map(_.get)
        (undeterminedSpecsFirst, determinedSpecsFirst) <- dbContext.db.run(crankshaft.dbBinding.findingExecutionSpecificationsForRevert(firstDeploymentRequest))

        // Can revert
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(thirdDeploymentRequest.id).map(_.get)
        (undeterminedSpecsThird, determinedSpecsThird) <- dbContext.db.run(crankshaft.dbBinding.findingExecutionSpecificationsForRevert(thirdDeploymentRequest))

      } yield {
        val specsThird = determinedSpecsThird.map { case (spec, targets) => spec.id -> (spec.version.value, targets) }.toMap
        (
          undeterminedSpecsFirst,
          determinedSpecsFirst.isEmpty,
          undeterminedSpecsThird.isEmpty,
          specsThird(firstExecSpecId),
          specsThird(secondExecSpecId)
        )
      }
    ) shouldBe(Set("moon", "mars"), true, true, (JsString("27").toString, Set("mars")), (JsString("54").toString, Set("moon")))
  }

  test("Crankshaft checks if an operation can be reverted") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("monkey")
        otherProduct <- crankshaft.insertProductIfNotExists("donkey")

        _ <- mockDeployExecution(product.name, "12", Map("orbit" -> Status.success))
        // Status = orbit: monkey@12

        (secondDeploymentRequest, _) <- mockDeployExecution(product.name, "55", Map("orbit" -> Status.success, "venus" -> Status.success))
        // Status = orbit: monkey@55, venus: monkey@55

        (thirdDeploymentRequest, _) <- mockDeployExecution(product.name, "69", Map("orbit" -> Status.success, "venus" -> Status.success))
        // Status = orbit: monkey@69, venus: monkey@69

        (otherDeploymentRequest, _) <- mockDeployExecution(otherProduct.name, "215", Map("orbit" -> Status.success, "venus" -> Status.success))
        // Status = orbit: monkey@69 + bonobo@215, venus: monkey@69 + bonobo@215

        // Second request can't be reverted
        secondDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(secondDeploymentRequest.id).map(_.get)
        rejectionOfSecond <- rejectIfCannot(Operation.revert, secondDeploymentRequest).failed
        // Third one can be reverted, because it's the last one for its product
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(thirdDeploymentRequest.id).map(_.get)
        _ <- rejectIfCannot(Operation.revert, thirdDeploymentRequest)
        // Meanwhile the deployment on another product can be reverted (even when it's the first one for that product: it just requires a default revert version)
        otherDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(otherDeploymentRequest.id).map(_.get)
        _ <- rejectIfCannot(Operation.revert, otherDeploymentRequest)

        (nothingDoneDeploymentRequest, _) <- mockDeployExecution(product.name, "51", Map("pluto" -> Status.notDone), initFailed = true, updateTargetStatuses = false)
        // Status = initFailed

        // Verify we can't revert a request if no targetStatuses exists
        nothingDoneDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(nothingDoneDeploymentRequest.id).map(_.get)
        rejectionOfNothingDone <- rejectIfCannot(Operation.revert, nothingDoneDeploymentRequest).failed

        (notDoneDeploymentRequest, _) <- mockDeployExecution(product.name, "51", Map("planet x" -> Status.notDone), initFailed = true)
        // Status = initFailed

        // Verify we can't revert a request if it has no effect
        notDoneDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(notDoneDeploymentRequest.id).map(_.get)
        rejectionOfNotDone <- rejectIfCannot(Operation.revert, notDoneDeploymentRequest).failed

      } yield List(rejectionOfSecond, rejectionOfNothingDone, rejectionOfNotDone).map(_.getMessage.split(":")(1).trim)
    ) shouldBe List("a newer one has already been applied", "Nothing to revert", "Nothing to revert")
  }

  test("Crankshaft rejects reverts of outdated deployment requests") {
    val dr1 = request("zealous-zebu", "Zzz", Seq("asia", "africa")).step().deploymentRequest
    val dr2 = request("zealous-zebu", "xXx", Seq("asia")).step().deploymentRequest
    val dr3 = request("zestful-zebra", "not-related", Seq("unknown-desert")).step().deploymentRequest
    request("zestful-zebra", "newer", Seq("unknown-desert"))
    await(
      for {
        msg <- crankshaft.revert(dr1, Some(1), "foo", None).failed.map(_.getMessage) // outdated by dr2
        _ <- crankshaft.revert(dr2, Some(1), "foo", None) // revert the last request for this product
        _ <- crankshaft.revert(dr3, Some(1), "foo", Some(Version(JsString("older")))) // not outdated because the following one is not started
      } yield msg
    ) should endWith("a newer one has already been applied")
  }

  test("Crankshaft rejects outdated revert intents before devising the plan") {
    val deploymentRequest = request("ocelot", "awesome-version", Seq("norway", "peru")).step().deploymentRequest
    await(
      for {
        msg <- crankshaft.revert(deploymentRequest, Some(0), "foo", None).failed.map(_.getMessage)
        _ <- crankshaft.revert(deploymentRequest, Some(1), "foo", Some(Version(JsString("first-version-ever"))))
      } yield msg
    ) should include("the state of the deployment has just changed")
  }

  test("Crankshaft performs a revert") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("pony")

        (firstDeploymentRequest, firstExecSpecId) <- mockDeployExecution(product.name, "11", Map("tic" -> Status.success, "tac" -> Status.success))
        // Status = tic: pony@11, tac: pony@11

        (secondDeploymentRequest, secondExecSpecId) <- mockDeployExecution(product.name, "22", Map("tic" -> Status.success, "tac" -> Status.success))
        secondDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(secondDeploymentRequest.id).map(_.get)
        // Status = tic: pony@22, tac: pony@22

        // Revert the last deployment request
        revertOperationTraceIdA <- mockRevertExecution(secondDeploymentRequest, Map("tic" -> Status.success, "tac" -> Status.hostFailure), None)
        revertExecutionSpecIdsA <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceIdA)
        // Status = tic: pony@11, tac: pony@11

        (thirdDeploymentRequest, thirdExecSpecId) <- mockDeployExecution(product.name, "33", Map("tic" -> Status.success, "tac" -> Status.success))
        // Status = tic: pony@33, tac: pony@33

        // Second request can't be reverted anymore
        rejectionOfSecondA <- rejectIfCannot(Operation.revert, secondDeploymentRequest).failed

        // Revert the last deployment request
        revertOperationTraceIdB <- mockRevertExecution(thirdDeploymentRequest, Map("tic" -> Status.success, "tac" -> Status.success), None)
        revertExecutionSpecIdsB <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceIdB)
        // Status = tic: pony@11, tac: pony@11

        // Second request still can't be reverted
        rejectionOfSecondB <- rejectIfCannot(Operation.revert, secondDeploymentRequest).failed
      } yield {
        revertExecutionSpecIdsA should have length 1
        revertExecutionSpecIdsA should contain(firstExecSpecId)

        rejectionOfSecondA.getMessage should include("a newer one has already been applied")

        revertExecutionSpecIdsB should have length 1
        revertExecutionSpecIdsB should contain(firstExecSpecId)

        rejectionOfSecondB.getMessage should include("a newer one has already been applied")
      }
    )
  }

  test("Crankshaft keeps track of retried operation") {
    await(
      for {
        product <- crankshaft.insertProductIfNotExists("martian")
        deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "robert"))
        operationTrace <- crankshaft.step(deploymentRequest, Some(0), "ignace")
        firstExecutionTraces <- closeOperation(operationTrace, Map("moon" -> Status.success, "mars" -> Status.hostFailure))
        retriedOperation <- crankshaft.step(deploymentRequest, Some(1), "b.lightning")
        secondExecutionTraces <- closeOperation(retriedOperation, Map("moon" -> Status.success, "mars" -> Status.success))
        raceConditionError <- crankshaft.step(deploymentRequest, Some(1), "b.lightning").failed
        hasOpenExecutionAfter <- hasOpenExecutionTracesForOperation(retriedOperation.id)
        operationReClosingSucceeded <- closeOperationTrace(retriedOperation)
        initialExecutionSpecIds <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(operationTrace.id)
        retriedExecutionSpecIds <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(retriedOperation.id)
      } yield {
        firstExecutionTraces.length shouldEqual 1
        secondExecutionTraces.length shouldEqual 1
        firstExecutionTraces.intersect(secondExecutionTraces).length shouldEqual 0
        retriedOperation.id == operationTrace.id shouldBe false
        hasOpenExecutionAfter shouldBe false
        operationReClosingSucceeded.isDefined shouldBe false
        initialExecutionSpecIds.length == retriedExecutionSpecIds.length shouldBe true
        initialExecutionSpecIds == retriedExecutionSpecIds shouldBe true
        raceConditionError should be(a[Conflict])
        raceConditionError.getMessage should include("the state of the deployment has just changed")
      }
    )
  }

  test("Crankshaft's binding provides the last version deployed on a given target") {
    def v(versionName: String): Version = Version(JsString(versionName))

    request("mournful-moray", "v13", Seq("paris", "london", "tokyo"))
      .step()
    request("pirate-piranha", "0.0.1", Seq("london", "tokyo"))
      .step()
    request("mournful-moray", "v14.doomed", Seq("paris", "tokyo"))
      .step(Status.notDone)
    val mm = request("mournful-moray", "v13.eu", Seq("paris", "london", "kuala lumpur"))
    mm.step(Status.productFailure)

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

    mm.revert("prewar")
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget("mournful-moray", Seq("paris", "london", "tokyo", "kuala lumpur")) should
      become(Map("paris" -> v("v13"), "london" -> v("v13"), "tokyo" -> v("v13"), "kuala lumpur" -> v("prewar")))
  }

  test("Crankshaft computes the dominant version when given an ordered sequence of reference pools") {
    def v(versionName: String): Option[Version] = Some(Version(JsString(versionName)))

    request("spatial-sparrow", "hot-fix", Seq("sun"))
      .step()
    request("side-siberian", "universal", Seq("sun", "earth", "saturn", "proxima"))
      .step()
    request("spatial-sparrow", "sunny", Seq("mercury", "venus", "earth", "mars"))
      .step()
    request("spatial-sparrow", "cold", Seq("jupiter", "saturn", "uranus", "neptune"))
      .step()
    request("side-siberian", "future", Seq("earth", "mars", "neptune"))
      .step()
    val sparrow = request("spatial-sparrow", "fantasy", Seq("earth", "mars", "moon"))
    sparrow.step()
    sparrow.revert("big-bang")
    request("spatial-sparrow", "no-op", Seq("mercury", "earth"))
      .step(Status.notDone)

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
      deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "bob"))
      operationTrace <- crankshaft.step(deploymentRequest, Some(0), "ignace")
      hasOpenExecution <- dbContext.db.run(crankshaft.dbBinding.hasOpenExecutionTracesForOperation(operationTrace.id))
      executionTrace <- crankshaft.dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequest.id).map(_.head)
    } yield (
      hasOpenExecution, executionTrace.state
    )
    res should become(false, ExecutionState.initFailed)
  }
}


class CrankshaftWithMultiStepSpec extends SimpleScenarioTesting {
  private val step1 = Set("north", "south")
  private val step2 = Set("east", "west")

  private def getDeployedVersions(productName: String) =
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget(productName, step1 ++ step2).map(_.mapValues(_.structured.head.value))

  test("Crankshaft can retry the first step if it's failing and revert it") {
    val r = request("enormous-elephant", "new", step1, step2)

    r.eligibleOperations should become(Seq(Operation.deploy))
    r.step(Status.productFailure)
    getDeployedVersions("enormous-elephant") should become(Map("north" -> "new", "south" -> "new"))

    r.eligibleOperations should become(Seq(Operation.deploy, Operation.revert))
    r.step()
    getDeployedVersions("enormous-elephant") should become(Map("north" -> "new", "south" -> "new"))

    r.eligibleOperations should become(Seq(Operation.deploy, Operation.revert))
    r.revert("old")
    getDeployedVersions("enormous-elephant") should become(Map("north" -> "old", "south" -> "old"))
  }

  test("Crankshaft cannot retry a successful deploy") {
    val r = request("fat-falcon", "new", step1, step2)

    r.eligibleOperations should become(Seq(Operation.deploy))
    r.step()
    getDeployedVersions("fat-falcon") should become(Map("north" -> "new", "south" -> "new"))

    r.eligibleOperations should become(Seq(Operation.deploy, Operation.revert))
    r.step()
    getDeployedVersions("fat-falcon") should become(Map("north" -> "new", "south" -> "new", "east" -> "new", "west" -> "new"))

    r.eligibleOperations should become(Seq(Operation.revert))
    (the[UnprocessableIntent] thrownBy r.step()).message should
      endWith("there is no next step, they have all been successfully deployed")
  }

  test("Crankshaft can retry a failed revert") {
    val r = request("giant-clam", "new", step1, step2)

    r.eligibleOperations should become(Seq(Operation.deploy))
    r.step()

    r.eligibleOperations should become(Seq(Operation.deploy, Operation.revert))
    r.revert("old", Status.hostFailure)

    r.eligibleOperations should become(Seq(Operation.revert))
    r.revert("older")
    getDeployedVersions("giant-clam") should become(Map("north" -> "older", "south" -> "older"))
  }

  test("Crankshaft cannot retry a successful revert") {
    val r = request("huge-human", "new", step1, step2)

    r.eligibleOperations should become(Seq(Operation.deploy))
    r.step()

    r.eligibleOperations should become(Seq(Operation.deploy, Operation.revert))
    r.revert("old")

    r.eligibleOperations should become(Seq[Operation.Kind]())
    (the[UnprocessableIntent] thrownBy r.revert("older")).message should
      endWith("there is no next step, they have all been successfully reverted")
  }

  test("Crankshaft cannot deploy anymore once it has been tentatively reverted") {
    val r = request("immense-impala", "new", step1, step2)

    r.eligibleOperations should become(Seq[Operation.Kind](Operation.deploy))
    r.step()

    r.eligibleOperations should become(Seq[Operation.Kind](Operation.deploy, Operation.revert))
    r.revert("old")

    r.eligibleOperations should become(Seq[Operation.Kind]())
    (the[UnprocessableIntent] thrownBy r.step()).message should
      endWith("deploying after a revert is not supported")
  }
}


class CrankshaftWithNoLogHrefSpec extends SimpleScenarioTesting {
  test("Cannot stop an execution that has no log href") {
    val op = request("dusty-duck", "12345", Seq("thailand")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(execTrace => crankshaft.stopExecution(execTrace, "joe")) should
      asynchronouslyThrow[RuntimeException]("No log href for execution trace .+")
  }
}


class CrankshaftWithStopperSpec extends SimpleScenarioTesting {
  private val logHref = "controllable.executions.io/42"

  override protected def triggerMock = Some(logHref)

  private val executionMock = mock[TriggeredExecution]
  when(executionMock.logHref).thenReturn(logHref)

  override val executionFinder = new TriggeredExecutionFinder(null) {
    override def apply[T](executionTrace: ShallowExecutionTrace): TriggeredExecution =
      executionMock
  }

  test("Stop a running execution") {
    when(executionMock.stopper).thenReturn(Some(() => None))

    val op = request("dusty-duck", "1234567", Seq("thailand")).startStep()
    Await.result(crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(crankshaft.stopExecution(_, "joe")), 1.second) shouldBe true
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id).map(_.map(t => (t.state, t.detail)).toSet) should
      become(Set((ExecutionState.aborted, "stopped by joe")))
    crankshaft.findDeploymentRequestAndEffects(op.deploymentRequest.id).map(_.get._3.map(_.operationTrace.closingDate.isDefined)) should
      become(Iterable(true))
  }

  test("Crankshaft tries to stop executions, which might terminate normally at the same time") {
    when(executionMock.stopper).thenReturn(Some(() => None))

    request("dusty-duck", "1", Seq("here")).step()
    val dep = request("dusty-duck", "2", Seq("here", "there")).step()

    // try to stop when everything is already terminated
    crankshaft.tryStopOperation(dep, "killer-guy") should
      eventually(be((0, Seq())))

    // try to stop when nothing has been terminated
    crankshaft.revert(dep.deploymentRequest, Some(1), "r.everter", Some(Version("0".toJson))).flatMap(op =>
      crankshaft.tryStopOperation(op, "killer-guy")
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.length)
          )
        }
    ) should eventually(be((2, 0, 2, 0))) // i.e. 2 execution traces, 0 closed, 2 stopped, 0 failures

    // try to stop when one execution is already terminated
    crankshaft.revert(dep.deploymentRequest, Some(2), "r.everter", Some(Version("0".toJson))).flatMap(op =>
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
        .flatMap(_ => crankshaft.tryStopOperation(op, "killer-guy"))
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.length)
          )
        }
    ) should eventually(be((2, 0, 1, 0))) // i.e. 2 execution traces (1 running, 1 completed), 0 closed, 1 stopped, 0 failure

    when(executionMock.stopper).thenReturn(Some(() => Some(ExecutionState.unreachable)))
    // try to stop when the job cannot be stopped
    crankshaft.revert(dep.deploymentRequest, None, "r.everter", Some(Version("0".toJson))).flatMap(op =>
      crankshaft.tryStopOperation(op, "killer-guy")
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures)
          )
        }
    ) should eventually(be((2, 0, 0, Vector(s"Could not stop the execution $logHref (current state: unreachable)", s"Could not stop the execution $logHref (current state: unreachable)"))))
    // i.e. 2 execution traces, 0 closed, 0 stopped, 2 failures
  }
}


class CrankshaftWithUncontrollableTriggeredExecutionSpec extends SimpleScenarioTesting {
  private val logHref = "uncontrollable.executions.io/42"

  override protected def triggerMock = Some(logHref)

  override val executionFinder = new TriggeredExecutionFinder(null) {
    override def apply[T](executionTrace: ShallowExecutionTrace): TriggeredExecution =
      new UncontrollableTriggeredExecution(logHref)
  }

  test("Cannot stop an execution of an explicitly unstoppable type") {
    val op = request("dusty-duck", "1234567", Seq("thailand")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(crankshaft.stopExecution(_, "joe")) should
      asynchronouslyThrow[RuntimeException](s"This kind of execution cannot be stopped: $logHref")
  }

  test("Crankshaft tries to stop executions, which might terminate normally at the same time") {
    request("dusty-duck", "1", Seq("here")).step()
    val dep = request("dusty-duck", "2", Seq("here", "there")).step()

    // try to stop when everything is already terminated
    crankshaft.tryStopOperation(dep, "killer-guy") should
      eventually(be((0, Seq())))

    // try to stop when nothing has been terminated but it's impossible to stop
    crankshaft.revert(dep.deploymentRequest, Some(1), "r.everter", Some(Version("0".toJson))).flatMap(op =>
      crankshaft.tryStopOperation(op, "killer-guy")
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op, initFailed = true).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.length)
          )
        }
    ) should eventually(be((2, 2, 0, 2))) // i.e. 2 execution traces, 2 closed, 0 stopped, 2 failures

    // try to stop when one execution is already terminated and the other one could not be stopped (so 0 success)
    crankshaft.revert(dep.deploymentRequest, Some(2), "r.everter", Some(Version("0".toJson))).flatMap(op =>
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
        .flatMap(_ => crankshaft.tryStopOperation(op, "killer-guy"))
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.map(_.split(":").head))
          )
        }
    ) should eventually(be((2, 1, 0, Seq("This kind of execution cannot be stopped")))) // i.e. 2 execution traces, 1 closed, 0 stopped, 1 failure
  }
}


class CrankshaftWithUnknownLogHrefSpec extends SimpleScenarioTesting {
  private val logHref = "now you can track me down"

  override protected def triggerMock = Some(logHref)

  test("A trivial execution triggers a job with a log href when a log href is provided") {
    val op = request("product #2", "1000", Seq("*")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.flatMap(_.logHref).toSet) should eventually(be(Set(logHref)))
  }

  test("Cannot stop an execution of an unknown type") {
    val op = request("dusty-duck", "123456", Seq("thailand")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(crankshaft.stopExecution(_, "joe")) should
      asynchronouslyThrow[RuntimeException]("Could not find an execution configuration for the type `unknown`")
  }
}
