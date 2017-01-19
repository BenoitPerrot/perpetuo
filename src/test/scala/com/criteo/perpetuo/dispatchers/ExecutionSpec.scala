package com.criteo.perpetuo.dispatchers

import java.sql.Timestamp

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.dispatchers.DeploymentRequestParser._
import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}
import com.twitter.inject.Test
import com.typesafe.config.{Config, ConfigFactory}
import slick.driver.JdbcDriver
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}

import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}
import scala.collection.immutable.Stream
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class ExecutionSpec extends Test with DeploymentRequestBinder with ProfileProvider {

  private val config: Config = ConfigFactory.load()
  private val dbModule = new TestingDbContextModule(config.getConfig("db").getConfig("test"))

  val profile: JdbcDriver = dbModule.driver

  import TestSuffixDispatcher._
  import profile.api._

  private val db = Database.forDataSource(dbModule.dataSourceProvider)
  new Schema(profile).createTables(db)

  private val dummyCounter = Stream.from(1).toIterator
  private val execLogs: ConcurrentMap[ExecutorInvoker, String] = new TrieMap()
  private val execution = new Execution(new ExecutionTraceBinding(dbModule.driver)) {
    override protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, rawTarget: String): Unit = {
      execLogs.put(executor, rawTarget).map(prev => fail(s"Logs say the executor has $rawTarget to do, but it already has $prev to do!"))
    }
  }

  object DummyInvokerWithUuid extends DummyInvoker("DummyWithUUID") {
    override def trigger(operation: Operation, executionId: Long, productName: String, version: String, rawTarget: String, initiator: String): Option[Future[String]] = {
      assert(super.trigger(operation, executionId, productName, version, rawTarget, initiator).isEmpty)
      Some(Future.successful(s"#${dummyCounter.next}"))
    }
  }

  private def getExecutions(dispatcher: TargetDispatching): Seq[(Long, Option[String])] = {
    val req = DeploymentRequest(None, "perpetuo-app", "v42", """"*"""", "No fear", "c.norris", new Timestamp(123456789))

    val (id, asyncStart) = execution.startTransaction(db, dispatcher, req)
    Await.ready(asyncStart, 2.seconds)
    assert(id.isCompleted)

    Await.result(
      id.flatMap(execution.executionTraces.findExecutionTracesByDeploymentRequest(db, _)),
      2.seconds
    ).map(exec => {
      assert(exec.id.isDefined)
      (exec.id.get, exec.uuid)
    })
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
      it.map { case (exec, raw) => (exec, parseTargetExpression(raw.parseJson)) }

    def dispatchedAs(that: Iterable[(ExecutorInvoker, TargetExpr)]): Unit = {
      parse(execution.dispatch(TestSuffixDispatcher, target)) should contain theSameElementsAs that

      execLogs.clear()
      Await.result(execution.startTransaction(db, TestSuffixDispatcher, request)._2, 2.seconds)
      parse(execLogs) should contain theSameElementsAs that
    }
  }


  "A trivial execution" should {
    "trigger a job with no ID when there is no UUID provided" in {
      getExecutions(DummyTargetDispatcher) shouldEqual Seq(
        (1, None)
      )
    }

    "trigger a job with an ID when a UUID is provided as a Future" in {
      getExecutions(SingleTargetDispatcher(DummyInvokerWithUuid)) shouldEqual Seq(
        (2, Some("#1"))
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

    "be dispatched as short target expressions" in {
      val Alternatives = Set
      execution.dispatchAlternatives(TestSuffixDispatcher, Set(
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("-baz")),
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("o-baz")),
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("oo-baz")),
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("foo-baz")),
        TargetTerm(select = Set("o-baz"))
      )).map {
        case (executor, representations) =>
          (executor, representations.map(repr => parseTargetExpression(repr.parseJson)))
      } should contain theSameElementsAs Map(
        fooInvoker -> Alternatives(
          Set(
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("-baz")),
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("o-baz" /*DIFF*/ , "oo-baz", "foo-baz")),
            TargetTerm(select = Set("o-baz"))
          ),
          Set(
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("-baz")),
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("oo-baz", "foo-baz")),
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05)) /*DIFF*/ , JsObject()), Set("o-baz"))
          )
        ),
        barInvoker -> Alternatives( // there is only one possible representation for such a simple expression
          Set(
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("-baz"))
          )
        )
      )
    }

  }


  "Target dispatching" should {
    "deduplicate targets and regenerate them (per executor) in a concise way" in {
      val targetWithDuplicates: TargetExpr = Set(
        TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def")),
        TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def")),
        TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc")),
        TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("def")),
        TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("ghi")),
        TargetTerm(Set(JsObject("foo" -> JsString("bar2"))), Set("ghi")),
        TargetTerm(Set(JsObject("foo2" -> JsString("bar"))), Set("ghi"))
      )
      val dispatchedTargets = execution
        .dispatch(DummyTargetDispatcher, targetWithDuplicates)
        .map(_._2)
        .map(_.parseJson)
        .map(parseTargetExpression)
      dispatchedTargets should contain theSameElementsAs Seq(
        Set(
          TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def", "ghi")),
          TargetTerm(Set(JsObject("foo" -> JsString("bar2")), JsObject("foo2" -> JsString("bar"))), Set("ghi"))
        )
      )
    }
  }
}
