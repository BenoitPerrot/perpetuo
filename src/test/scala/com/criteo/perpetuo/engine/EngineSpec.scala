package com.criteo.perpetuo.engine

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.auth.{Unrestricted, User}
import com.criteo.perpetuo.model._
import com.google.common.base.Ticker
import com.google.common.cache.{CacheBuilder, LoadingCache}
import spray.json.JsString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class EngineSpec extends SimpleScenarioTesting {

  object MockTicker extends Ticker {
    private val nanos = new AtomicLong

    def advance(time: Long, timeUnit: TimeUnit): this.type = {
      nanos.addAndGet(timeUnit.toNanos(time))
      this
    }

    def advanceTime(): Future[Unit] = {
      nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2001))
      Future.successful(())
    }

    override def read: Long = nanos.getAndAdd(0)
  }

  override lazy val engine: Engine = new Engine(crankshaft, Unrestricted) {
    override protected val cachedState: LoadingCache[java.lang.Long, Future[Option[DeploymentState]]] =
      CacheBuilder.newBuilder()
        .maximumSize(128)
        .expireAfterAccess(2, TimeUnit.SECONDS)
        .concurrencyLevel(10)
        .ticker(MockTicker)
        .build(stateCacheLoader)
  }

  private def closeOperationTrace(operationTrace: OperationTrace): Future[Option[OperationTrace]] =
    dbContext.db.run(dbBinding.closingOperationTrace(operationTrace))

  private val someone = User("s.omeone")
  private val starter = User("s.tarter")

  test("Testing the cache is keeping state for 2 seconds") {
    await(
      for {
        product <- engine.upsertProduct(starter, "product #1")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        op <- engine.step(starter, depReq.id, Some(0)).map(_.get)

        stillNotStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)

        _ <- MockTicker.advanceTime()
        onGoing <- engine.findDeploymentRequestState(depReq.id).map(_.get)

        _ <- dbBinding.findExecutionTracesByOperationTrace(op.id)
          .map(_.map(execTrace => engine.tryUpdateExecutionTrace(execTrace.id, ExecutionState.completed, "", None)))

        stillOnGoing <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ <- MockTicker.advanceTime()

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
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", someone.name))
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
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ <- engine.step(starter, depReq.id, None)
        depReqStatus <- engine.queryDeploymentRequestStatus(Some(starter), depReq.id).map(_.get)
        execTraceId = depReqStatus.operationEffects.head.executionTraces.head.id
        _ <- engine.tryUpdateExecutionTrace(execTraceId, ExecutionState.aborted, "", None)
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
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", someone.name))
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
        err shouldBe a[Conflict]
        finalState shouldBe a[DeployInProgress]
      }
    )
  }

}
