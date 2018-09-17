package com.criteo.perpetuo.engine

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.auth.{Unrestricted, User}
import com.criteo.perpetuo.model.{ExecutionState, ProtoDeploymentPlanStep, ProtoDeploymentRequest, Version}
import com.google.common.base.Ticker
import com.google.common.cache.CacheBuilder
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
    override protected val cachedState = CacheBuilder.newBuilder()
      .maximumSize(128)
      .expireAfterAccess(2, TimeUnit.SECONDS)
      .concurrencyLevel(10)
      .ticker(MockTicker)
      .build(stateCacheLoader)
  }

  val someone = User("s.omeone")
  val starter = User("s.tarter")

  test("Testing the cache is keeping state for 2 seconds") {

    await(
      for {
        product <- engine.upsertProduct(starter, "product #1")
        depReq <- engine.requestDeployment(someone, ProtoDeploymentRequest(product.name, Version("\"1000\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "", someone.name))
        notStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        operationTrace <- engine.step(starter, depReq.id, Some(0))

        stillNotStarted <- engine.findDeploymentRequestState(depReq.id).map(_.get)

        _ <- MockTicker.advanceTime()
        onGoing <- engine.findDeploymentRequestState(depReq.id).map(_.get)

        _ <- engine.findExecutionTracesByDeploymentRequest(depReq.id)
          .map(_.get.map(execTrace => engine.tryUpdateExecutionTrace(execTrace.id, ExecutionState.completed, "", None)))

        stillOnGoing <- engine.findDeploymentRequestState(depReq.id).map(_.get)
        _ <- MockTicker.advanceTime()

        completed <- engine.findDeploymentRequestState(depReq.id).map(_.get)

      } yield {
        notStarted.isInstanceOf[NotStarted] shouldBe true
        stillNotStarted.isInstanceOf[NotStarted] shouldBe true
        onGoing.isInstanceOf[DeployInProgress] shouldBe true
        stillOnGoing.isInstanceOf[DeployInProgress] shouldBe true
        completed.isInstanceOf[DeployFlopped] shouldBe true
      }
    )

  }

}
