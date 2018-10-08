package com.criteo.perpetuo.engine

import com.criteo.perpetuo.engine.executors._
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{SimpleScenarioTesting, TestTargetResolver}
import org.mockito.Mockito.when
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// TODO: extract the most Engine-oriented tests into an EngineSpec
class CrankshaftSpec extends SimpleScenarioTesting {

  private def hasOpenExecutionTracesForOperation(operationTraceId: Long) =
    dbContext.db.run(crankshaft.dbBinding.hasOpenExecutionTracesForOperation(operationTraceId))

  private def closeOperationTrace(operationTrace: OperationTrace): Future[Option[OperationTrace]] =
    dbContext.db.run(crankshaft.dbBinding.closingOperationTrace(operationTrace))

  private def findCurrentVersionForEachKnownTarget(productName: String, amongAtoms: Iterable[String]) =
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget(productName, amongAtoms.map(TargetAtom))
      .map(_.map { case (k, v) => k.name -> v })

  private def computeDominantVersion(productName: String, referenceAtoms: Iterable[Iterable[String]]) =
    crankshaft.computeDominantVersion(productName, referenceAtoms.map(_.map(TargetAtom))).map(_.map { version =>
      val v :: Nil = version.structured
      v.value
    })

  test("A trivial execution triggers a job with no href when there is no href provided") {
    await(
      for {
        product <- crankshaft.upsertProduct("product #1")
        depPlan <- crankshaft.dbBinding.insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", "s.omeone"))
        NotStarted(_, deploymentPlanSteps, effectsBeforeStart, stepBeforeStart) <- crankshaft.assessDeploymentState(depPlan.deploymentRequest).map(_.asInstanceOf[NotStarted])
        op <- step(depPlan.deploymentRequest, Some(0), "s.tarter")
        DeployInProgress(_, _, effectsAfterStart, inProgress) <- crankshaft.assessDeploymentState(depPlan.deploymentRequest).map(_.asInstanceOf[DeployInProgress])
        traces <- crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      } yield {
        traces.map(trace => (trace.id, trace.href)) shouldEqual Seq((1, None))
        effectsBeforeStart shouldBe empty
        stepBeforeStart shouldBe deploymentPlanSteps.head
        effectsAfterStart shouldNot be(empty)
        inProgress.deploymentPlanStepIds shouldBe Seq(stepBeforeStart.id)
      }
    )
  }

  test("Crankshaft keeps track of open executions for an operation") {
    await(
      for {
        product <- crankshaft.upsertProduct("human")
        deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "robert"))
        operationTrace <- step(deploymentRequest, Some(0), "ignace")
        hasOpenExecutionBefore <- hasOpenExecutionTracesForOperation(operationTrace.id)
        _ <- closeOperation(operationTrace, Map("moon" -> Status.success, "mars" -> Status.hostFailure))
        hasOpenExecutionAfter <- hasOpenExecutionTracesForOperation(operationTrace.id)
        operationReClosingSucceeded <- closeOperationTrace(operationTrace)
      } yield {
        hasOpenExecutionBefore shouldBe true
        hasOpenExecutionAfter shouldBe false
        operationReClosingSucceeded shouldNot be(defined)
      }
    )
  }

  def mockDeployExecution(productName: String, v: String, targetAtomToStatus: Map[String, Status.Code], updateTargetStatuses: Boolean = true): Future[(DeploymentRequest, Long)] = {
    for {
      deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(productName, Version(JsString(v)), Seq(ProtoDeploymentPlanStep("", targetAtomToStatus.keys.toJson, "")), "", "r.equestor"))
      operationTrace <- step(deploymentRequest, Some(0), "s.tarter")
      executionSpecIds <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(operationTrace.id)
      _ <- closeOperation(operationTrace, if (updateTargetStatuses) targetAtomToStatus else Map())
    } yield (deploymentRequest, executionSpecIds.head)
  }

  def mockRevertExecution(deploymentRequest: DeploymentRequest, targetAtomToStatus: Map[String, Status.Code], defaultVersion: Option[Version] = None): Future[Long] = {
    for {
      operationTrace <- revert(deploymentRequest, None, "r.everter", defaultVersion)
      _ <- closeOperation(operationTrace, targetAtomToStatus)
    } yield operationTrace.id
  }

  test("Crankshaft checks that an operation can be started only if previous transactions on the same product have been completed") {
    await(
      for {
        product <- crankshaft.upsertProduct("pig")

        // OK if it's the first
        _ <- mockDeployExecution(product.name, "99", Map("racing" -> Status.success))

        // OK after a success
        (second, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.hostFailure))

        // not OK if it's after a deployment failure
        conflictMsg <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))
          .map(_ => "unrejected").recover { case Conflict(msg, _, _) => msg }

        // the failing one must be reverted first
        _ <- mockRevertExecution(second, Map("corn-field" -> Status.hostFailure), Some(Version(JsString("big-bang"))))

        // OK after the failing has been reverted (even if the revert failed)
        (third, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success))

        // note that we can revert a successful operation
        _ <- mockRevertExecution(third, Map("racing" -> Status.success))

        // OK after a revert of a successful operation
        _ <- mockDeployExecution(product.name, "102", Map("corn-field" -> Status.notDone))

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
        product <- crankshaft.upsertProduct("horse")

        (firstDeploymentRequest, _) <- mockDeployExecution(product.name, "100", Map("corn-field" -> Status.success))
        // Status = corn-field: horse@100

        (secondDeploymentRequest, _) <- mockDeployExecution(product.name, "101", Map("racing" -> Status.success, "pool" -> Status.success))
        // Status = corn-field: horse@100, racing: horse@101, pool: horse@101

        (thirdDeploymentRequest, _) <- mockDeployExecution(product.name, "102", Map("racing" -> Status.productFailure))
        // Status = corn-field: horse@100, racing: horse@102?, pool: horse@101

        // fixme: one day, first deployment will be retryable:
        // But second one can't be, because it impacts `racing`, whose status changed in the meantime
        secondDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(secondDeploymentRequest.id).map(_.get)
        secondState <- crankshaft.assessDeploymentState(secondDeploymentRequest)
        // The last one of course is retryable
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(thirdDeploymentRequest.id).map(_.get)
        thirdState <- crankshaft.assessDeploymentState(thirdDeploymentRequest)
      } yield {
        secondState shouldBe an[Outdated]
        thirdState shouldBe a[DeployFailed]
      }
    )
  }

  test("Crankshaft finds executions for reverting") {
    await(
      for {
        product <- crankshaft.upsertProduct("mouse")

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
        val specsThird = determinedSpecsThird.map { case (spec, targets) => spec.id -> (spec.version.serialized, targets) }.toMap
        undeterminedSpecsFirst.items.map(_.name) shouldEqual Set("moon", "mars")
        determinedSpecsFirst shouldBe empty
        undeterminedSpecsThird.items shouldBe empty
        specsThird(firstExecSpecId) shouldEqual(JsString("27").toString, TargetAtomSet(Set(TargetAtom("mars"))))
        specsThird(secondExecSpecId) shouldEqual(JsString("54").toString, TargetAtomSet(Set(TargetAtom("moon"))))
      }
    )
  }

  test("Crankshaft checks if an operation can be reverted") {
    await(
      for {
        product <- crankshaft.upsertProduct("monkey")
        otherProduct <- crankshaft.upsertProduct("donkey")

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
        secondState <- crankshaft.assessDeploymentState(secondDeploymentRequest)
        // Third one can be reverted, because it's the last one for its product
        thirdDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(thirdDeploymentRequest.id).map(_.get)
        thirdState <- crankshaft.assessDeploymentState(thirdDeploymentRequest)
        // Meanwhile the deployment on another product can be reverted (even when it's the first one for that product: it just requires a default revert version)
        otherDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(otherDeploymentRequest.id).map(_.get)
        otherState <- crankshaft.assessDeploymentState(otherDeploymentRequest)

        (nothingDoneDeploymentRequest, _) <- mockDeployExecution(product.name, "51", Map("pluto" -> Status.notDone), updateTargetStatuses = false)
        // Status = early failure

        // Verify we can't revert a request if no targetStatuses exists
        nothingDoneDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(nothingDoneDeploymentRequest.id).map(_.get)
        nothingDoneState <- crankshaft.assessDeploymentState(nothingDoneDeploymentRequest)

        (notDoneDeploymentRequest, _) <- mockDeployExecution(product.name, "51", Map("planet x" -> Status.notDone))
        // Status = early failure

        // Verify we can't revert a request if it has no effect
        notDoneDeploymentRequest <- crankshaft.dbBinding.findDeploymentRequestById(notDoneDeploymentRequest.id).map(_.get)
        notDoneState <- crankshaft.assessDeploymentState(notDoneDeploymentRequest)

      } yield {
        secondState shouldBe an[Outdated]
        thirdState shouldBe a[Deployed]
        otherState shouldBe a[Deployed]
        nothingDoneState shouldBe a[DeployFlopped]
        notDoneState shouldBe a[DeployFlopped]
      }
    )
  }

  test("Crankshaft rejects reverts of outdated deployment requests") {
    val dr1 = request("zealous-zebu", "Zzz", Seq("asia", "africa")).step().deploymentRequest
    val dr2 = request("zealous-zebu", "xXx", Seq("asia")).step().deploymentRequest
    val dr3 = request("zestful-zebra", "not-related", Seq("unknown-desert")).step().deploymentRequest
    request("zestful-zebra", "newer", Seq("unknown-desert"))
    await(
      for {
        msg <- revert(dr1, Some(1), "foo", None).failed.map(_.asInstanceOf[RejectingException].msg) // outdated by dr2
        _ <- revert(dr2, Some(1), "foo", None) // revert the last request for this product
        _ <- revert(dr3, Some(1), "foo", Some(Version(JsString("older")))) // not outdated because the following one is not started
      } yield {
        msg shouldEqual "a newer one has already been applied"
      }
    )
  }

  test("Crankshaft rejects outdated revert intents before devising the plan") {
    val deploymentRequest = request("ocelot", "awesome-version", Seq("norway", "peru")).step().deploymentRequest
    await(
      for {
        msg <- revert(deploymentRequest, Some(0), "foo", None).failed.map(_.getMessage)
        _ <- revert(deploymentRequest, Some(1), "foo", Some(Version(JsString("first-version-ever"))))
      } yield {
        msg should include("the state of the deployment has just changed")
      }
    )
  }

  test("Crankshaft performs a revert") {
    await(
      for {
        product <- crankshaft.upsertProduct("pony")

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
        stateOfSecondA <- crankshaft.assessDeploymentState(secondDeploymentRequest)

        // Revert the last deployment request
        revertOperationTraceIdB <- mockRevertExecution(thirdDeploymentRequest, Map("tic" -> Status.success, "tac" -> Status.success), None)
        revertExecutionSpecIdsB <- crankshaft.dbBinding.findExecutionSpecIdsByOperationTrace(revertOperationTraceIdB)
        // Status = tic: pony@11, tac: pony@11

        // Second request still can't be reverted
        stateOfSecondB <- crankshaft.assessDeploymentState(secondDeploymentRequest)
      } yield {
        revertExecutionSpecIdsA should have length 1
        revertExecutionSpecIdsA should contain(firstExecSpecId)

        stateOfSecondA shouldBe an[Outdated]

        revertExecutionSpecIdsB should have length 1
        revertExecutionSpecIdsB should contain(firstExecSpecId)

        stateOfSecondB shouldBe an[Outdated]
      }
    )
  }

  test("Crankshaft keeps track of retried operation") {
    await(
      for {
        product <- crankshaft.upsertProduct("martian")
        deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "robert"))
        operationTrace <- step(deploymentRequest, Some(0), "ignace")
        firstExecutionTraces <- closeOperation(operationTrace, Map("moon" -> Status.success, "mars" -> Status.hostFailure))
        retriedOperation <- step(deploymentRequest, Some(1), "b.lightning")
        secondExecutionTraces <- closeOperation(retriedOperation, Map("moon" -> Status.success, "mars" -> Status.success))
        raceConditionError <- step(deploymentRequest, Some(1), "b.lightning").failed
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
    findCurrentVersionForEachKnownTarget("pirate-piranha", Seq("paris", "london", "tokyo")) should
      become(Map("london" -> v("0.0.1"), "tokyo" -> v("0.0.1"))) // no "paris"

    // if a version failed to start, it still counts as the last deployed version
    findCurrentVersionForEachKnownTarget("mournful-moray", Seq("london", "paris", "new-york")) should
      become(Map("paris" -> v("v13.eu"), "london" -> v("v13.eu"))) // no "new-york"

    // if a version has not been actually deployed on a target (i.e. despite the request, see status `notDone`)
    // it must not be considered as the last deployed version on the target
    findCurrentVersionForEachKnownTarget("mournful-moray", Seq("tokyo")) should
      become(Map("tokyo" -> v("v13")))

    mm.revert("prewar")
    findCurrentVersionForEachKnownTarget("mournful-moray", Seq("paris", "london", "tokyo", "kuala lumpur")) should
      become(Map("paris" -> v("v13"), "london" -> v("v13"), "tokyo" -> v("v13"), "kuala lumpur" -> v("prewar")))
  }

  test("Crankshaft computes the dominant version when given an ordered sequence of reference pools") {
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

    computeDominantVersion("spatial-sparrow", Seq(
      Seq("unknown"))
    ) should become[Option[String]](None)

    computeDominantVersion("spatial-sparrow", Seq(
      Seq("sun"))
    ) should become[Option[String]](Some("hot-fix"))

    computeDominantVersion("spatial-sparrow", Seq(
      Seq("sun", "mercury", "venus"), // hot-fix, sunny, sunny
      Seq("jupiter", "saturn", "uranus", "neptune") // cold, cold, cold, cold
    )) should become[Option[String]](Some("sunny"))

    computeDominantVersion("spatial-sparrow", Seq(
      Seq("unknown", "target"), //
      Seq("mercury", "venus"), // sunny, sunny
      Seq("jupiter", "saturn", "uranus", "neptune") // cold, cold, cold, cold
    )) should become[Option[String]](Some("sunny"))

    computeDominantVersion("spatial-sparrow", Seq(
      Seq("moon")
    )) should become[Option[String]](Some("big-bang"))

    computeDominantVersion("spatial-sparrow", Seq(
      Seq("earth", "mars", "uranus"), // sunny (from revert), sunny (from revert), cold
      Seq("jupiter", "saturn", "neptune") // cold, cold, cold
    )) should become[Option[String]](Some("sunny"))
  }
}


class CrankshaftWithFailingExecutorSpec extends SimpleScenarioTesting {
  override protected def triggerMock = throw new RuntimeException("too bad, dude")

  test("Crankshaft keeps the created records in DB and marks an execution trace as failed if the trigger fails") {
    await(
      for {
        product <- crankshaft.upsertProduct("airplane")
        deploymentRequest <- crankshaft.createDeploymentRequest(ProtoDeploymentRequest(product.name, Version(JsString("42").compactPrint), Seq(ProtoDeploymentPlanStep("", JsArray(JsString("moon"), JsString("mars")), "")), "", "bob"))
        operationTrace <- step(deploymentRequest, Some(0), "ignace")
        hasOpenExecution <- dbContext.db.run(crankshaft.dbBinding.hasOpenExecutionTracesForOperation(operationTrace.id))
        executionTraces <- crankshaft.dbBinding.findExecutionTracesByOperationTrace(operationTrace.id)
      } yield {
        hasOpenExecution shouldBe false
        executionTraces.map(_.state) shouldEqual Seq(ExecutionState.initFailed)
      }
    )
  }
}


class CrankshaftWithFailingTargetSpec extends SimpleScenarioTesting {
  override val resolver: TargetResolver = TestTargetResolver

  private val step1 = Set("north", "south")
  private val step2 = Set("east-west")

  private def getDeployedVersions(productName: String) =
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget(productName, (step1 ++ step2).map(TargetAtom))
      .map(_.map { case (k, v) => k.name -> v.structured.head.value })

  private def findTargetsByOperationTrace(op: OperationTrace) = {
    crankshaft.dbBinding.findExecutionIdsByOperationTrace(op.id)
      .flatMap { executionIds =>
        val executionId = executionIds.head
        findTargetsByExecution(executionId).map(_.map(_.name))
      }
  }

  test("Crankshaft retries on failed targets only") {
    val r = request("big-brother", "new", step1, step2)

    r.eligibleOperations should become(Seq(Operation.deploy))
    r.step(Map("north" -> Status.success, "south" -> Status.productFailure))
    getDeployedVersions("big-brother") should become(Map("north" -> "new", "south" -> "new"))

    val op = r.step()
    findTargetsByOperationTrace(op) should become(Seq("south"))

    r.step(Map("east" -> Status.success, "west" -> Status.productFailure))
    val op2 = r.step()
    findTargetsByOperationTrace(op2) should become(Seq("west"))
  }
}


class CrankshaftWithDynamicResolutionSpec extends SimpleScenarioTesting {
  var targetToAtoms: Map[TargetNonAtom, Set[TargetAtom]] = _
  override val resolver: TargetResolver = new TargetResolver {
    override def resolveTerms(productName: String, productVersion: Version, targetTerms: Set[TargetNonAtom]): Option[Map[TargetNonAtom, Set[TargetAtom]]] = {
      Some(targetToAtoms)
    }
  }

  private def findTargetsByOperationTrace(op: OperationTrace) = {
    crankshaft.dbBinding.findExecutionIdsByOperationTrace(op.id)
      .flatMap { executionIds =>
        val executionId = executionIds.head
        findTargetsByExecution(executionId).map(_.map(_.name).toSet)
      }
  }

  test("Crankshaft retries on failed targets only") {
    val r = request("big-brother", "new", Set("world"))

    targetToAtoms = Map(TargetWord("world") -> Set(TargetAtom("europe"), TargetAtom("asia"), TargetAtom("africa")))
    r.step(Map("europe" -> Status.success, "asia" -> Status.productFailure, "africa" -> Status.productFailure))

    // Target resolution changes in the second run
    targetToAtoms = Map(TargetWord("world") -> Set(TargetAtom("europe"), TargetAtom("asia")))
    val op = r.step()
    findTargetsByOperationTrace(op) should become(Set("asia"))
  }

  test("A retry can deploy on new nodes") {
    val r = request("big-brother", "newer", Set("world"))

    targetToAtoms = Map(TargetWord("world") -> Set(TargetAtom("europe"), TargetAtom("asia")))
    r.step(Map("europe" -> Status.productFailure, "asia" -> Status.success))

    targetToAtoms = Map(TargetWord("world") -> Set(TargetAtom("europe"), TargetAtom("asia"), TargetAtom("africa")))
    val op = r.step()

    findTargetsByOperationTrace(op) should become(Set("europe", "africa"))
  }
}


class CrankshaftWithMultiStepSpec extends SimpleScenarioTesting {
  private val step1 = Set("north", "south")
  private val step2 = Set("east", "west")

  private def getDeployedVersions(productName: String) =
    crankshaft.dbBinding.findCurrentVersionForEachKnownTarget(productName, (step1 ++ step2).map(TargetAtom))
      .map(_.map { case (k, v) => k.name -> v.structured.head.value })

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
    (the[UnavailableAction] thrownBy r.step()).msg shouldBe "the deployment transaction is closed"
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
    (the[UnavailableAction] thrownBy r.revert("older")).msg shouldBe "the deployment transaction is closed"
  }

  test("Crankshaft cannot deploy anymore once it has been tentatively reverted") {
    val r = request("immense-impala", "new", step1, step2)

    r.eligibleOperations should become(Seq[Operation.Kind](Operation.deploy))
    r.step()

    r.eligibleOperations should become(Seq[Operation.Kind](Operation.deploy, Operation.revert))
    r.revert("old")

    r.eligibleOperations should become(Seq[Operation.Kind]())
    (the[UnavailableAction] thrownBy r.step()).msg shouldBe "the deployment transaction is closed"
  }

  test("Crankshaft retries on all unresolvable targets") {
    val r = request("colossal-beast", "new", step1, step2)

    r.eligibleOperations should become(Seq(Operation.deploy))
    r.step(Map("north" -> Status.success, "south" -> Status.productFailure))

    r.eligibleOperations should become(Seq(Operation.deploy, Operation.revert))

    val op = r.step()

    crankshaft.dbBinding.findExecutionIdsByOperationTrace(op.id)
      .flatMap { executionIds =>
        val executionId = executionIds.head
        findTargetsByExecution(executionId).map(_.map(_.name))
      } should become(Seq("north", "south"))
  }
}


class CrankshaftWithNoHrefSpec extends SimpleScenarioTesting {
  test("Cannot stop an execution that has no href") {
    val op = request("dusty-duck", "12345", Seq("thailand")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(execTrace => crankshaft.stopExecution(execTrace, "joe")) should
      asynchronouslyThrow[RuntimeException]("No href for execution trace .+")
  }
}


class CrankshaftWithStopperSpec extends SimpleScenarioTesting {
  private val href = "controllable.executions.io/42"

  override protected def triggerMock = Some(href)

  private val executionMock = mock[TriggeredExecution]
  when(executionMock.href).thenReturn(href)

  override val executionFinder: TriggeredExecutionFinder = new TriggeredExecutionFinder(null) {
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
    crankshaft.dbBinding.dbContext.db.run(crankshaft.dbBinding.findingDeploymentRequestAndEffects(op.deploymentRequest.id)).map(_.get._3.map(_.operationTrace.closingDate.isDefined)) should
      become(Iterable(true))
  }

  test("Crankshaft tries to stop executions, which might terminate normally at the same time") {
    when(executionMock.stopper).thenReturn(Some(() => None))

    request("dusty-duck", "1", Seq("here")).step()
    val dep = request("dusty-duck", "2", Seq("here", "there")).step()

    // try to stop when everything is already terminated
    tryStopOperation(dep, "killer-guy") should
      eventually(be((0, Seq())))

    // try to stop when nothing has been terminated
    revert(dep.deploymentRequest, Some(1), "r.everter", Some(Version("0".toJson))).flatMap(op =>
      tryStopOperation(op, "killer-guy")
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.length)
          )
        }
    ) should eventually(be((2, 0, 2, 0))) // i.e. 2 execution traces, 0 closed, 2 stopped, 0 failures

    // try to stop when one execution is already terminated
    revert(dep.deploymentRequest, Some(2), "r.everter", Some(Version("0".toJson))).flatMap(op =>
      crankshaft.dbBinding.findExecutionIdsByOperationTrace(op.id)
        .flatMap { executionIds =>
          val executionId = executionIds.head // only update the first execution (out of the 2 triggered by the revert)
          findTargetsByExecution(executionId).flatMap(atoms =>
            crankshaft.dbBinding.findExecutionTraceIdsByExecution(executionId).flatMap(executionTraceIds =>
              Future.traverse(executionTraceIds)(executionTraceId =>
                crankshaft.updateExecutionTrace(
                  executionTraceId, ExecutionState.aborted, "from the executor", None,
                  atoms.map(_ -> TargetAtomStatus(Status.success, "")).toMap
                ))
            ))
        }
        .flatMap(_ => tryStopOperation(op, "killer-guy"))
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.length)
          )
        }
    ) should eventually(be((2, 0, 1, 0))) // i.e. 2 execution traces (1 running, 1 completed), 0 closed, 1 stopped, 0 failure

    when(executionMock.stopper).thenReturn(Some(() => Some(ExecutionState.unreachable)))
    // try to stop when the job cannot be stopped
    revert(dep.deploymentRequest, None, "r.everter", Some(Version("0".toJson))).flatMap(op =>
      tryStopOperation(op, "killer-guy")
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures)
          )
        }
    ) should eventually(be((2, 0, 0, Vector(s"Could not stop the execution $href (current state: unreachable)", s"Could not stop the execution $href (current state: unreachable)"))))
    // i.e. 2 execution traces, 0 closed, 0 stopped, 2 failures
  }
}


class CrankshaftWithUncontrollableTriggeredExecutionSpec extends SimpleScenarioTesting {
  private val href = "uncontrollable.executions.io/42"

  override protected def triggerMock = Some(href)

  override val executionFinder: TriggeredExecutionFinder = new TriggeredExecutionFinder(null) {
    override def apply[T](executionTrace: ShallowExecutionTrace): TriggeredExecution =
      new UncontrollableTriggeredExecution(href)
  }

  test("Cannot stop an execution of an explicitly unstoppable type") {
    val op = request("dusty-duck", "1234567", Seq("thailand")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(crankshaft.stopExecution(_, "joe")) should
      asynchronouslyThrow[RuntimeException](s"This kind of execution cannot be stopped: $href")
  }

  test("Crankshaft tries to stop executions, which might terminate normally at the same time") {
    request("dusty-duck", "1", Seq("here")).step()
    val dep = request("dusty-duck", "2", Seq("here", "there")).step()

    // try to stop when everything is already terminated
    tryStopOperation(dep, "killer-guy") should
      eventually(be((0, Seq())))

    // try to stop when nothing has been terminated but it's impossible to stop
    revert(dep.deploymentRequest, Some(1), "r.everter", Some(Version("0".toJson))).flatMap(op =>
      tryStopOperation(op, "killer-guy")
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op, Map("here" -> Status.notDone)).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.length)
          )
        }
    ) should eventually(be((2, 2, 0, 2))) // i.e. 2 execution traces, 2 closed, 0 stopped, 2 failures

    // try to stop when one execution is already terminated and the other one could not be stopped (so 0 success)
    revert(dep.deploymentRequest, Some(2), "r.everter", Some(Version("0".toJson))).flatMap(op =>
      crankshaft.dbBinding.findExecutionIdsByOperationTrace(op.id)
        .flatMap { executionIds =>
          val executionId = executionIds.head // only update the first execution (out of the 2 triggered by the revert)
          findTargetsByExecution(executionId).flatMap(atoms =>
            crankshaft.dbBinding.findExecutionTraceIdsByExecution(executionId).flatMap(executionTraceIds =>
              Future.traverse(executionTraceIds)(executionTraceId =>
                crankshaft.updateExecutionTrace(
                  executionTraceId, ExecutionState.aborted, "from the executor", None,
                  atoms.map(_ -> TargetAtomStatus(Status.success, "")).toMap
                ))
            ))
        }
        .flatMap(_ => tryStopOperation(op, "killer-guy"))
        .flatMap { case (successes, failures) =>
          tryCloseOperation(op).map(updates =>
            (updates.length, updates.flatten.length, successes, failures.map(_.split(":").head))
          )
        }
    ) should eventually(be((2, 1, 0, Seq("This kind of execution cannot be stopped")))) // i.e. 2 execution traces, 1 closed, 0 stopped, 1 failure
  }
}


class CrankshaftWithUnknownHrefSpec extends SimpleScenarioTesting {
  private val href = "now you can track me down"

  override protected def triggerMock = Some(href)

  test("A trivial execution triggers a job with a href when a href is provided") {
    val op = request("product #2", "1000", Seq("*")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.flatMap(_.href).toSet) should eventually(be(Set(href)))
  }

  test("Cannot stop an execution of an unknown type") {
    val op = request("dusty-duck", "123456", Seq("thailand")).step()
    crankshaft.dbBinding.findExecutionTracesByOperationTrace(op.id)
      .map(_.head)
      .flatMap(crankshaft.stopExecution(_, "joe")) should
      asynchronouslyThrow[RuntimeException]("Could not find an execution configuration for the type `unknown`")
  }
}
