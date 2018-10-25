package com.criteo.perpetuo.engine

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.auth.User
import com.criteo.perpetuo.model._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class EngineSpec extends SimpleScenarioTesting {
  private def closeOperationTrace(operationTrace: OperationTrace): Future[Option[OperationTrace]] =
    dbContext.db.run(dbBinding.closingOperationTrace(operationTrace))

  private val someone = User("s.omeone")
  private val starter = User("s.tarter")
  private val releaser = User("r.eleaser")

  private def tryUpdateExecutionTraces(engine: Engine, executionTraces: Iterable[ShallowExecutionTrace],
                                       state: ExecutionState.Value, detail: String = "", href: Option[String] = None, statusMap: Map[TargetAtom, TargetAtomStatus] = Map()) =
    Future.sequence(
      executionTraces.map(executionTrace => engine.tryUpdateExecutionTrace(executionTrace.id, state, detail, href, statusMap))
    )

  test("The deployment state is cached for some definite time") {
    await(
      for {
        product <- engine.upsertProduct(starter, "product #1")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("atom"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        op <- engine.step(starter, depReq.id, Some(0)).map(_.get)

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
        depReq <- engine.crankshaft.findDeploymentRequestById(depReq.id).map(_.get) // Refresh the Deployment request instance
        abandoned <- engine.crankshaft.assessDeploymentState(depReq)
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
        depReq <- engine.crankshaft.findDeploymentRequestById(depReq.id).map(_.get) // Refresh the Deployment request instance
        flopped <- engine.crankshaft.assessDeploymentState(depReq)
        _ <- engine.abandon(someone, depReq.id)
        depReq <- engine.crankshaft.findDeploymentRequestById(depReq.id).map(_.get)
        abandoned <- engine.crankshaft.assessDeploymentState(depReq)
      } yield {
        notStarted shouldBe a[NotStarted]
        flopped shouldBe a[DeployFlopped]
        abandoned shouldBe a[Abandoned]
      }
    )
  }

  test("A request which is not started nor flopped cannot be abandoned") {
    await(
      for {
        product <- engine.upsertProduct(starter, "product")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("atom"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        operationTrace <- engine.step(starter, depReq.id, None).map(_.get)
        depReq <- engine.crankshaft.findDeploymentRequestById(depReq.id).map(_.get)
        inProgress <- engine.crankshaft.assessDeploymentState(depReq)
        err <- engine.abandon(starter, depReq.id).failed
        depReq <- engine.crankshaft.findDeploymentRequestById(depReq.id).map(_.get)
        finalState <- engine.crankshaft.assessDeploymentState(depReq)
        _ <- closeOperationTrace(operationTrace)
      } yield {
        notStarted shouldBe a[NotStarted]
        inProgress shouldBe a[DeployInProgress]
        err shouldBe a[OperationLockAlreadyTaken]
        finalState shouldBe a[DeployInProgress]
      }
    )
  }

  test("Finding deployment request states works") {
    await(
      for {
        product <- engine.upsertProduct(starter, "multi-state")

        r0 <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("0".toJson), Seq(ProtoDeploymentPlanStep("", JsString("lcy"), "")), "", someone.name))
        _ <- engine.step(releaser, r0.id, None).map(_.get)
        inProgress0 <- engine.crankshaft.assessDeploymentState(r0)
        _ <- tryUpdateExecutionTraces(engine, inProgress0.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("lcy") -> TargetAtomStatus(Status.success, "")))

        states0 <- engine.findDeploymentRequestsStates(Seq(Map("field" -> "productName", "equals" -> "multi-state")), 10, 0)

        r1 <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("1".toJson), Seq(ProtoDeploymentPlanStep("", JsString("lcy"), "")), "", someone.name))
        _ <- engine.step(releaser, r1.id, None).map(_.get)
        inProgress1 <- engine.crankshaft.assessDeploymentState(r1)
        _ <- tryUpdateExecutionTraces(engine, inProgress1.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("lcy") -> TargetAtomStatus(Status.productFailure, "")))
        _ <- engine.revert(releaser, r1.id, None, None).map(_.get)
        revertInProgress <- engine.crankshaft.assessDeploymentState(r1)
        _ <- tryUpdateExecutionTraces(engine, revertInProgress.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("lcy") -> TargetAtomStatus(Status.success, "")))

        _ = mockTicker.jump()
        states10 <- engine.findDeploymentRequestsStates(Seq(Map("field" -> "productName", "equals" -> "multi-state")), 10, 0)

        r2 <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("2".toJson), Seq(ProtoDeploymentPlanStep("", JsString("lcy"), "")), "", someone.name))
        _ <- engine.step(releaser, r2.id, None).map(_.get)

        _ = mockTicker.jump()
        states21 <- engine.findDeploymentRequestsStates(Seq(Map("field" -> "productName", "equals" -> "multi-state")), 2, 0)
      } yield {
        states0.size shouldBe 1
        states0.head shouldBe an[Deployed]

        states10.size shouldBe 2
        states10(1) shouldBe an[Outdated]
        states10.head shouldBe a[Reverted]

        states21.size shouldBe 2
        states21(1) shouldBe an[Outdated]
        states21.head shouldBe a[DeployInProgress]
      }
    )
  }

  test("Step-stammering while running is idempotent") {
    await(
      for {
        product <- engine.upsertProduct(starter, "step-stammering")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("2".toJson), Seq(ProtoDeploymentPlanStep("", JsString("cdg"), "")), "", someone.name))

        operationTrace0 <- engine.step(releaser, depReq.id, Some(0)).map(_.get)
        inProgress0 <- engine.crankshaft.assessDeploymentState(depReq)
        step0Again <- engine.step(releaser, depReq.id, Some(0)).map(_.get)

        step0AgainForAnotherUser <- engine.step(someone, depReq.id, Some(0)).map(_.get).failed
        step1Locked <- engine.step(releaser, depReq.id, Some(1)).map(_.get).failed
        revert0Locked <- engine.revert(releaser, depReq.id, Some(0), None).map(_.get).failed

        _ <- tryUpdateExecutionTraces(engine, inProgress0.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("cdg") -> TargetAtomStatus(Status.success, "")))

        _ <- engine.revert(releaser, depReq.id, Some(1), Some(Version("1".toJson))).map(_.get)
        inProgress1 <- engine.crankshaft.assessDeploymentState(depReq)
        nextStep1Locked <- engine.step(releaser, depReq.id, Some(1)).map(_.get).failed
      } yield {
        inProgress0 shouldBe a[DeployInProgress]
        step0Again shouldBe operationTrace0

        step0AgainForAnotherUser shouldBe a[OperationLockAlreadyTaken]
        step1Locked shouldBe a[OperationLockAlreadyTaken]
        revert0Locked shouldBe a[OperationLockAlreadyTaken]

        inProgress1 shouldBe a[RevertInProgress]
        nextStep1Locked shouldBe a[OperationLockAlreadyTaken]
      }
    )
  }

  test("Out-of-order stepping is idempotent") {
    await(
      for {
        product <- engine.upsertProduct(starter, "out-of-order-stepping")
        depReq <- engine.requestDeployment(someone,
          ProtoDeploymentRequest(product.name, Version("2".toJson), Seq(
            ProtoDeploymentPlanStep("", JsString("nrt"), ""),
            ProtoDeploymentPlanStep("", JsString("hnd"), "")),
          "", someone.name))

        operationTrace0 <- engine.step(releaser, depReq.id, Some(0)).map(_.get)
        inProgress0 <- engine.crankshaft.assessDeploymentState(depReq)
        _ <- tryUpdateExecutionTraces(engine, inProgress0.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("nrt") -> TargetAtomStatus(Status.success, "")))

        operationTrace1 <- engine.step(releaser, depReq.id, Some(1)).map(_.get)
        inProgress1 <- engine.crankshaft.assessDeploymentState(depReq)
        _ <- tryUpdateExecutionTraces(engine, inProgress1.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(TargetAtom("hnd") -> TargetAtomStatus(Status.success, "")))

        step1Again <- engine.step(releaser, depReq.id, Some(1)).map(_.get)

        step0Again <- engine.step(releaser, depReq.id, Some(0)).map(_.get)

        operationTrace2 <- engine.revert(releaser, depReq.id, Some(2), Some(Version("1".toJson))).map(_.get)
        inProgress2 <- engine.crankshaft.assessDeploymentState(depReq)
        _ <- tryUpdateExecutionTraces(engine, inProgress2.effects.head.executionTraces, ExecutionState.completed, statusMap = Map(
          TargetAtom("nrt") -> TargetAtomStatus(Status.success, ""),
          TargetAtom("hnd") -> TargetAtomStatus(Status.success, ""))
        )
        reverted <- engine.crankshaft.assessDeploymentState(depReq)

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
