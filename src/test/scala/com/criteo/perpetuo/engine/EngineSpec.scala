package com.criteo.perpetuo.engine

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.model.DeploymentRequestState._
import com.criteo.perpetuo.model._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class EngineSpec extends SimpleScenarioTesting {
  private def closeOperationTrace(operationTrace: OperationTrace): Future[Boolean] =
    dbContext.db.run(dbBinding.closingOperationTrace(operationTrace)).map { case (_, updated) => updated }

  private val someone = User("s.omeone")
  private val starter = User("s.tarter")
  private val releaser = User("r.eleaser")

  test("The deployment state is cached for some definite time") {
    await(
      for {
        product <- engine.upsertProduct(starter, "product #1")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("atom"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ <- engine.step(starter, depReq.id, Some(0)).map(_.get)

        stillNotStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)

        _ = mockTicker.jump()
        onGoing <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ <- tryUpdateExecutionTraces(engine, onGoing.effects.head.executionTraces, ExecutionState.completed)

        stillOnGoing <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ = mockTicker.jump()

        completed <- engine.findDeploymentRequestState(depReq.id).map(_.get)
      } yield {
        notStarted shouldBe a[NotStarted]
        stillNotStarted shouldBe a[NotStarted]
        onGoing shouldBe a[DeployInProgress]
        stillOnGoing shouldBe a[DeployInProgress]
        completed shouldBe a[DeployFlopped]
      }
    )
  }

  test("A not started request is abandoned") {
    await(
      for {
        product <- engine.upsertProduct(starter, "product")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("atom"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ <- engine.abandon(someone, depReq.id)
        depReq <- crankshaft.findDeploymentRequestById(depReq.id).map(_.get) // Refresh the Deployment request instance
        abandoned <- crankshaft.assessDeploymentState(depReq)
      } yield {
        notStarted shouldBe a[NotStarted]
        abandoned shouldBe a[Abandoned]
      }
    )
  }

  test("A flopped request is abandoned") {
    await(
      for {
        product <- engine.upsertProduct(starter, "product")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("atom"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ <- engine.step(starter, depReq.id, None)
        (state, _) <- engine.queryDeploymentRequestStatus(Some(starter), depReq.id).map(_.get)
        _ <- tryUpdateExecutionTraces(engine, state.effects.head.executionTraces, ExecutionState.aborted)
        depReq <- crankshaft.findDeploymentRequestById(depReq.id).map(_.get) // Refresh the Deployment request instance
        flopped <- crankshaft.assessDeploymentState(depReq)
        _ <- engine.abandon(someone, depReq.id)
        depReq <- crankshaft.findDeploymentRequestById(depReq.id).map(_.get)
        abandoned <- crankshaft.assessDeploymentState(depReq)
      } yield {
        notStarted shouldBe a[NotStarted]
        flopped shouldBe a[DeployFlopped]
        abandoned shouldBe a[Abandoned]
      }
    )
  }

  test("A request which is started and not flopped cannot be abandoned") {
    await(
      for {
        product <- engine.upsertProduct(starter, "product")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("atom"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        operationTrace <- engine.step(starter, depReq.id, None).map(_.get)
        depReq <- crankshaft.findDeploymentRequestById(depReq.id).map(_.get)
        inProgress <- crankshaft.assessDeploymentState(depReq)
        err <- engine.abandon(starter, depReq.id).failed
        depReq <- crankshaft.findDeploymentRequestById(depReq.id).map(_.get)
        finalState <- crankshaft.assessDeploymentState(depReq)
        updatedOnClose <- closeOperationTrace(operationTrace)
      } yield {
        notStarted shouldBe a[NotStarted]
        inProgress shouldBe a[DeployInProgress]
        err shouldBe a[OperationLockAlreadyTaken]
        finalState shouldBe a[DeployInProgress]
        updatedOnClose shouldBe true
      }
    )
  }

  test("Finding deployment request states works") {
    await(
      for {
        product <- engine.upsertProduct(starter, "multi-state")

        r0 <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("0".toJson), Seq(ProtoDeploymentPlanStep("", JsString("lcy"), "")), "", someone.name))
        _ <- engine.step(releaser, r0.id, None).map(_.get)
        inProgress0 <- crankshaft.assessDeploymentState(r0)
        _ <- tryUpdateExecutionTraces(engine, inProgress0.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("lcy") -> TargetAtomStatus(Status.success, "")))

        states0 <- engine.findDeploymentRequestsAndPlan(Seq(Map("field" -> "productName", "equals" -> "multi-state")), 10, 0).map(_.map(_.deploymentRequest.state.get))

        r1 <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("1".toJson), Seq(ProtoDeploymentPlanStep("", JsString("lcy"), "")), "", someone.name))
        _ <- engine.step(releaser, r1.id, None).map(_.get)
        inProgress1 <- crankshaft.assessDeploymentState(r1)
        _ <- tryUpdateExecutionTraces(engine, inProgress1.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("lcy") -> TargetAtomStatus(Status.productFailure, "")))
        _ <- engine.revert(releaser, r1.id, None, None).map(_.get)
        revertInProgress <- crankshaft.assessDeploymentState(r1)
        _ <- tryUpdateExecutionTraces(engine, revertInProgress.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("lcy") -> TargetAtomStatus(Status.success, "")))

        _ = mockTicker.jump()
        states10 <- engine.findDeploymentRequestsAndPlan(Seq(Map("field" -> "productName", "equals" -> "multi-state")), 10, 0).map(_.map(_.deploymentRequest.state.get))
        r2 <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("2".toJson), Seq(ProtoDeploymentPlanStep("", JsString("lcy"), "")), "", someone.name))
        _ <- engine.step(releaser, r2.id, None).map(_.get)

        _ = mockTicker.jump()
        states21 <- engine.findDeploymentRequestsAndPlan(Seq(Map("field" -> "productName", "equals" -> "multi-state")), 2, 0).map(_.map(_.deploymentRequest.state.get))
      } yield {
        states0.size shouldBe 1
        states0.head shouldEqual deployed

        states10.size shouldBe 2
        states10 shouldBe Seq(reverted, deployed)

        states21.size shouldBe 2
        states21 shouldBe Seq(deployInProgress, reverted)
      }
    )
  }

  test("Step-stammering while running is idempotent") {
    await(
      for {
        product <- engine.upsertProduct(starter, "step-stammering")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("2".toJson), Seq(ProtoDeploymentPlanStep("", JsString("cdg"), "")), "", someone.name))

        operationTrace0 <- engine.step(releaser, depReq.id, Some(0)).map(_.get)
        inProgress0 <- crankshaft.assessDeploymentState(depReq)
        step0Again <- engine.step(releaser, depReq.id, Some(0)).map(_.get)

        step0AgainForAnotherUser <- engine.step(someone, depReq.id, Some(0)).map(_.get).failed
        step1Locked <- engine.step(releaser, depReq.id, Some(1)).map(_.get).failed
        revert0Locked <- engine.revert(releaser, depReq.id, Some(0), None).map(_.get).failed

        _ <- tryUpdateExecutionTraces(engine, inProgress0.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("cdg") -> TargetAtomStatus(Status.success, "")))

        _ <- engine.revert(releaser, depReq.id, Some(1), Some(Version("1".toJson))).map(_.get)
        inProgress1 <- crankshaft.assessDeploymentState(depReq)
        nextStep1Locked <- engine.step(releaser, depReq.id, Some(1)).map(_.get).failed
      } yield {
        inProgress0 shouldBe a[DeployInProgress]
        step0Again.id shouldBe operationTrace0.id

        step0AgainForAnotherUser shouldBe a[OperationLockAlreadyTaken]
        step1Locked shouldBe a[OperationLockAlreadyTaken]
        revert0Locked shouldBe a[OperationLockAlreadyTaken]

        inProgress1 shouldBe a[RevertInProgress]
        nextStep1Locked shouldBe a[OperationLockAlreadyTaken]
      }
    )
  }

  test("Out-of-order stepping is idempotent") {
    val steps = Seq(
      ProtoDeploymentPlanStep("", JsString("nrt"), ""),
      ProtoDeploymentPlanStep("", JsString("hnd"), "")
    )
    await(
      for {
        product <- engine.upsertProduct(starter, "out-of-order-stepping")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("2".toJson), steps, "", someone.name))

        operationTrace0 <- engine.step(releaser, depReq.id, Some(0)).map(_.get)
        inProgress0 <- crankshaft.assessDeploymentState(depReq)
        _ <- tryUpdateExecutionTraces(engine, inProgress0.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("nrt") -> TargetAtomStatus(Status.success, "")))

        operationTrace1 <- engine.step(releaser, depReq.id, Some(1)).map(_.get)
        inProgress1 <- crankshaft.assessDeploymentState(depReq)
        _ <- tryUpdateExecutionTraces(engine, inProgress1.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("hnd") -> TargetAtomStatus(Status.success, "")))

        step1Again <- engine.step(releaser, depReq.id, Some(1)).map(_.get)

        step0Again <- engine.step(releaser, depReq.id, Some(0)).map(_.get)

        operationTrace2 <- engine.revert(releaser, depReq.id, Some(2), Some(Version("1".toJson))).map(_.get)
        inProgress2 <- crankshaft.assessDeploymentState(depReq)
        _ <- tryUpdateExecutionTraces(engine, inProgress2.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(
          TargetAtom("nrt") -> TargetAtomStatus(Status.success, ""),
          TargetAtom("hnd") -> TargetAtomStatus(Status.success, ""))
        )
        reverted <- crankshaft.assessDeploymentState(depReq)

        step1AgainAgain <- engine.step(releaser, depReq.id, Some(1)).map(_.get)

        step0AgainAgain <- engine.step(releaser, depReq.id, Some(0)).map(_.get)

        revert2Again <- engine.revert(releaser, depReq.id, Some(2), Some(Version("1".toJson))).map(_.get)
      } yield {
        inProgress0 shouldBe a[DeployInProgress]
        step0Again.id shouldBe operationTrace0.id

        inProgress1 shouldBe a[DeployInProgress]
        step1Again.id shouldBe operationTrace1.id

        reverted shouldBe a[Reverted]
        revert2Again.id shouldBe operationTrace2.id

        step0AgainAgain.id shouldBe operationTrace0.id
        step1AgainAgain.id shouldBe operationTrace1.id
      }
    )
  }

  test("Finding the latest deployed version for one target") {
    val productName = "product1"
    request(productName, "1", Seq("atom")).step(Map("atom" -> Status.success))
    request(productName, "2", Seq("atom")).step(Map("atom" -> Status.success))
    val versions = engine.getCurrentVersionPerTarget(productName)
    versions should become(
      Map(TargetAtom("atom") -> Version("\"2\""))
    )
  }

  test("Finding the latest deployed version for multiple targets") {
    val productName = "product2"
    request(productName, "2", Seq("atom")).step(Map("atom" -> Status.hostFailure))
    request(productName, "1", Seq("par")).step(Map("par" -> Status.notDone))
    request(productName, "4", Seq("target1", "target2")).step(Map("target1" -> Status.productFailure, "target2" -> Status.success))

    val versions = engine.getCurrentVersionPerTarget(productName)
    versions should become(
      Map(
        TargetAtom("atom") -> Version("\"2\""),
        TargetAtom("target1") -> Version("\"4\""),
        TargetAtom("target2") -> Version("\"4\"")
      )
    )
  }

  test("Finding the latest deployed version for non completed targets") {
    val productName = "product3"
    request(productName, "1", Seq("atom")).step(Map("atom" -> Status.success))
    request(productName, "2", Seq("atom")).step(Map("atom" -> Status.notDone))
    val versions = engine.getCurrentVersionPerTarget(productName)
    versions should become(
      Map(TargetAtom("atom") -> Version("\"1\""))
    )
  }
}

class EngineAutoRevertSpec extends SimpleScenarioTesting {

  import dbContext.profile.api._

  def setDeploymentRequestAutoRevert(id: Long, autoRevert: Boolean): Future[Int] =
    dbContext.db.run(dbBinding.deploymentRequestQuery.filter(_.id === id).map(_.autoRevert).update(autoRevert))

  test("Auto-revert succeeds for a request that has a previous deployed version") {
    val productName = "ifrit"

    request(productName, "v0", Seq("terra")).step()

    val r = request(productName, "v1", Seq("terra"))
    await(setDeploymentRequestAutoRevert(r.getDeploymentRequest.id, autoRevert = true))
    r.step(Status.hostFailure)

    await(engine.autoRevertFailingDeploymentRequests)
    r.state shouldEqual revertInProgress
  }

  test("Auto-revert is not applied for a request that is not auto-revertible") {
    val productName = "leviathan"

    request(productName, "v0", Seq("terra")).step()

    val r = request(productName, "v1", Seq("terra"))
    await(setDeploymentRequestAutoRevert(r.getDeploymentRequest.id, autoRevert = false))
    r.step(Status.hostFailure)

    await(engine.autoRevertFailingDeploymentRequests)
    r.state shouldEqual deployFailed
  }

  test("Auto-revert fails for a request that has no previous deployed versions") {
    val productName = "eidolon"
    val r = request(productName, "v1", Seq("mist"))
    await(setDeploymentRequestAutoRevert(r.getDeploymentRequest.id, autoRevert = true))
    r.step(Status.hostFailure)

    await(engine.autoRevertFailingDeploymentRequests)
    r.state shouldEqual deployFailed

    r.step(Status.success) // Cleanup
  }

  test("Auto-revert is applied only for requests in a DeployFailed state") {
    val product1 = "p1"
    val product2 = "p2"
    val product3 = "p3"
    request(product1, "v0", Seq("par")).step()
    request(product2, "v0", Seq("par")).step()
    request(product3, "v0", Seq("par")).step()
    val r1 = request(product1, "v1", Seq("par"))
    val r2 = request(product2, "v1", Seq("par"))
    val r3 = request(product3, "v1", Seq("par"))
    await(setDeploymentRequestAutoRevert(r1.getDeploymentRequest.id, autoRevert = true))
    await(setDeploymentRequestAutoRevert(r2.getDeploymentRequest.id, autoRevert = true))
    await(setDeploymentRequestAutoRevert(r3.getDeploymentRequest.id, autoRevert = true))

    r1.step(Status.success)
    r2.step(Status.productFailure)
    await(engine.step(User("s.pidey"), r3.getDeploymentRequest.id, None))

    await(crankshaft.findAutoRevertibleDeploymentRequestIdsAndStateStamps).map(_._1) shouldEqual Vector(r2.getDeploymentRequest.id)
    await(engine.autoRevertFailingDeploymentRequests)

    r1.state shouldEqual deployed
    r2.state shouldEqual revertInProgress
    r3.state shouldEqual deployInProgress
  }
}
