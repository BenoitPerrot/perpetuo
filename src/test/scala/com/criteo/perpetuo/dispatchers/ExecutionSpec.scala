package com.criteo.perpetuo.dispatchers

import java.sql.Timestamp

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.dao.enums.Operation
import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.executors.DummyInvoker
import com.twitter.inject.Test
import com.typesafe.config.{Config, ConfigFactory}
import slick.driver.JdbcDriver
import spray.json.JsObject

import scala.collection.immutable.Stream
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class ExecutionSpec extends Test with DeploymentRequestBinder with ProfileProvider {

  private val config: Config = ConfigFactory.load()
  private val dbModule = new TestingDbContextModule(config.getConfig("db").getConfig("embedded"))

  val profile: JdbcDriver = dbModule.driver
  import profile.api._

  private val db = Database.forDataSource(dbModule.dataSourceProvider)
  new Schema(profile).createTables(db)

  private val dummyCounter = Stream.from(1).toIterator
  private val execution = new Execution(new ExecutionTraceBinding(dbModule.driver))

  object DummyInvokerWithUuid extends DummyInvoker("DummyWithUUID") {
    override def trigger(operation: Operation, executionId: Long, productName: String, version: String, tactics: Tactics, select: Select, initiator: String): Option[Future[String]] = {
      assert(super.trigger(operation, executionId, productName, version, tactics, select, initiator).isEmpty)
      Some(Future.successful(s"#${dummyCounter.next}"))
    }
  }

  private val req = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))
  private lazy val request = req.copy(id = Some(Await.result(insert(db, req), 2.seconds)))

  private def messagesGeneratedBy(dispatcher: TargetDispatching) = {
    val invocations = dispatcher.dispatch(Seq("abc", "def")).toSeq
    Await.result(execution.trigger(db, invocations, Operation.deploy, request, Seq(JsObject())), 2.seconds)
  }


  "A trivial execution" should {

    "trigger a job with no ID when there is no UUID provided" in {
      messagesGeneratedBy(DummyTargetDispatcher) shouldEqual Seq(
        "Triggered job with unknown ID for execution #1"
      )
    }

    "trigger a job with an ID when a UUID is provided as a Future" in {
      messagesGeneratedBy(SingleTargetDispatcher(DummyInvokerWithUuid)) shouldEqual Seq(
        "Triggered job `#1` for execution #2"
      )
    }

  }
}
