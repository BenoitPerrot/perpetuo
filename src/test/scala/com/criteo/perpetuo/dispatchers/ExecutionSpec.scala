package com.criteo.perpetuo.dispatchers

import java.sql.Timestamp

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.dao.enums.Operation
import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}
import com.twitter.inject.Test
import com.typesafe.config.{Config, ConfigFactory}
import slick.driver.JdbcDriver
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Stream
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class ExecutionSpec extends Test with DeploymentRequestBinder with ProfileProvider {

  private val config: Config = ConfigFactory.load()
  private val dbModule = new TestingDbContextModule(config.getConfig("db").getConfig("embedded"))

  val profile: JdbcDriver = dbModule.driver

  import TestSuffixDispatcher._
  import profile.api._

  private val db = Database.forDataSource(dbModule.dataSourceProvider)
  new Schema(profile).createTables(db)

  private val dummyCounter = Stream.from(1).toIterator
  private val execution = new Execution(new ExecutionTraceBinding(dbModule.driver))

  object DummyInvokerWithUuid extends DummyInvoker("DummyWithUUID") {
    override def trigger(operation: Operation, executionId: Long, productName: String, version: String, rawTarget: String, initiator: String): Option[Future[String]] = {
      assert(super.trigger(operation, executionId, productName, version, rawTarget, initiator).isEmpty)
      Some(Future.successful(s"#${dummyCounter.next}"))
    }
  }

  private val req = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))
  private lazy val request = req.copy(id = Some(Await.result(insert(db, req), 2.seconds)))

  private def messagesGeneratedBy(dispatcher: TargetDispatching) = {
    val invocations = dispatcher.assign("foo").toSeq.map((_, "rawFoo"))
    Await.result(execution.trigger(db, invocations, Operation.deploy, request, Seq(JsObject())), 2.seconds)
  }

  implicit class SimpleDispatchTest(private val select: Select) {
    private val rawTarget = Seq(TargetTerm(select = select)).toJson.compactPrint

    def dispatchedAs(that: Iterable[(ExecutorInvoker, Select)]): Unit = {
      val expected = that.map { case (e, s) => (e, Seq(TargetTerm(select = s)).toJson) }

      val dispatched = execution.dispatch(TestSuffixDispatcher, Seq(TargetTerm(select = select))).map {
        case (exec, repr) => (exec, repr.parseJson)
      }
      dispatched should contain theSameElementsAs expected
    }
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


  "A complex execution" should {

    "call the right executor when available for each exact target word" in {
      Seq("foo-baz", "foo-foo-baz", "bar-baz") dispatchedAs Seq(
        (fooInvoker, Seq("foo-baz")),
        (fooFooInvoker, Seq("foo-foo-baz")),
        (barInvoker, Seq("bar-baz"))
      )
    }

    "call the root set of pretty much all executors for all unknown targets" in {
      Seq("abc", "def") dispatchedAs Seq(
        (fooInvoker, Seq("abc", "def")),
        (barInvoker, Seq("abc", "def"))
      )
    }

    "call the same executor in one shot when applicable" in {
      Seq("o-baz", "oo-baz") dispatchedAs Seq(
        (fooInvoker, Seq("oo-baz", "o-baz"))
      )
    }

    "gather target words on executors when possible and still distribute unknown target words" in {
      Seq("o-baz", "-baz", "oo-baz") dispatchedAs Seq(
        (fooInvoker, Seq("oo-baz", "-baz", "o-baz")),
        (barInvoker, Seq("-baz"))
      )
    }

  }
}
