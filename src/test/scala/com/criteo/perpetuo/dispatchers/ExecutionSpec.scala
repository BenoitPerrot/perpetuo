package com.criteo.perpetuo.dispatchers

import java.sql.Timestamp

import com.criteo.perpetuo.dao._
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
  private var execLogs: List[(ExecutorInvoker, String)] = Nil
  private val execution = new Execution(new ExecutionTraceBinding(dbModule.driver)) {
    override protected def logExecution(msg: String, executor: ExecutorInvoker, rawTarget: String): Unit = {
      execLogs = (executor, rawTarget) :: execLogs
    }
  }

  object DummyInvokerWithUuid extends DummyInvoker("DummyWithUUID") {
    override def trigger(operation: Operation, executionId: Long, productName: String, version: String, rawTarget: String, initiator: String): Option[Future[String]] = {
      assert(super.trigger(operation, executionId, productName, version, rawTarget, initiator).isEmpty)
      Some(Future.successful(s"#${dummyCounter.next}"))
    }
  }

  private val req = DeploymentRequest(None, "perpetuo-app", "v42", """"*"""", "No fear", "c.norris", new Timestamp(123456789))

  private def messagesGeneratedBy(dispatcher: TargetDispatching) = {
    Await.result(execution.startTransaction(db, dispatcher, req)._2, 2.seconds)
  }

  implicit class SimpleDispatchTest(private val select: Select) {
    def dispatchedAs(that: Iterable[(ExecutorInvoker, Select)]): Unit = {
      Set(TargetTerm(select = select)) dispatchedAs that.map { case (e, s) => (e, Set(TargetTerm(select = s))) }
    }
  }

  implicit class ComplexDispatchTest(private val target: TargetExpr) {
    private val rawTarget = target.toJson.compactPrint
    private val request = DeploymentRequest(None, "perpetuo-app", "v42", rawTarget, "No fear", "c.norris", new Timestamp(123456789))

    private def parse(it: Iterable[(ExecutorInvoker, String)]): Iterable[(ExecutorInvoker, TargetExpr)] =
      it.map { case (exec, raw) => (exec, DeploymentRequestParser.parseTargetExpression(raw.parseJson)) }

    def dispatchedAs(that: Iterable[(ExecutorInvoker, TargetExpr)]): Unit = {
      parse(execution.dispatch(TestSuffixDispatcher, target)) should contain theSameElementsAs that

      execLogs = Nil
      val messages = Await.result(execution.startTransaction(db, TestSuffixDispatcher, request)._2, 2.seconds)
      messages.length shouldEqual that.size
      parse(execLogs) should contain theSameElementsAs that
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
      Set("foo-baz", "foo-foo-baz", "bar-baz") dispatchedAs Map(
        fooInvoker -> Set("foo-baz"),
        fooFooInvoker -> Set("foo-foo-baz"),
        barInvoker -> Set("bar-baz")
      )
    }

    "call the root set of pretty much all executors for all unknown targets" in {
      Set("abc", "def") dispatchedAs Map(
        fooInvoker -> Set("abc", "def"),
        barInvoker -> Set("abc", "def")
      )
    }

    "call the same executor in one shot when applicable" in {
      Set("o-baz", "oo-baz") dispatchedAs Map(
        fooInvoker -> Set("oo-baz", "o-baz")
      )
    }

    "gather target words on executors when possible and still distribute unknown target words" in {
      Set("o-baz", "-baz", "oo-baz") dispatchedAs Map(
        fooInvoker -> Set("oo-baz", "-baz", "o-baz"),
        barInvoker -> Set("-baz")
      )
    }

  }


  "A complex target expression" should {

    "be appropriately dispatched among impacted executors" in {
      Set(
        TargetTerm(select = Set("o-baz", "-baz", "oo-baz")),
        TargetTerm(
          Set(
            JsObject(
              "ratio" -> JsNumber(0.05),
              "awesome" -> JsTrue
            ),
            JsObject(
              "tag" -> JsString("canary"),
              "ratio" -> JsNumber(0.05)
            )
          ),
          Set("bar-baz", "-baz", "foo-foo-baz")
        )
      ) dispatchedAs Map(
        fooInvoker -> Set(
          TargetTerm(select = Set("o-baz", "oo-baz")),
          TargetTerm(
            Set(
              JsObject(),
              JsObject(
                "ratio" -> JsNumber(0.05),
                "awesome" -> JsTrue
              ),
              JsObject(
                "tag" -> JsString("canary"),
                "ratio" -> JsNumber(0.05)
              )
            ),
            Set("-baz")
          )
        ),
        barInvoker -> Set(
          TargetTerm(select = Set("-baz")),
          TargetTerm(
            Set(
              JsObject(
                "ratio" -> JsNumber(0.05),
                "awesome" -> JsTrue
              ),
              JsObject(
                "tag" -> JsString("canary"),
                "ratio" -> JsNumber(0.05)
              )
            ),
            Set("bar-baz", "-baz")
          )
        ),
        fooFooInvoker -> Set(
          TargetTerm(
            Set(
              JsObject(
                "ratio" -> JsNumber(0.05),
                "awesome" -> JsTrue
              ),
              JsObject(
                "tag" -> JsString("canary"),
                "ratio" -> JsNumber(0.05)
              )
            ),
            Set("foo-foo-baz")
          )
        )
      )
    }

  }
}
