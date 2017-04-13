package com.criteo.perpetuo.dispatchers

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}
import com.criteo.perpetuo.model.DeploymentRequestParser._
import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Operation, Product, Version}
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}

import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}
import scala.collection.immutable.Stream
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class ExecutionSpec extends Test with TestDb {

  import TestSuffixDispatcher._

  private val dummyCounter = Stream.from(1).toIterator
  private val execLogs: ConcurrentMap[ExecutorInvoker, TargetExpr] = new TrieMap()
  private val execution = new Execution(new DbBinding(dbContext)) {
    override protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, target: TargetExpr): Unit = {
      execLogs.put(executor, target).map(prev => fail(s"Logs say the executor has ${target.toJson.compactPrint} to do, but it already has $prev to do!"))
    }
  }

  object DummyInvokerWithLogHref extends DummyInvoker("DummyWithLogHref") {
    override def trigger(operationName: String, executionId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Option[Future[String]] = {
      assert(super.trigger(operationName, executionId, productName, version, target, initiator).isEmpty)
      Some(Future.successful(s"#${dummyCounter.next}"))
    }
  }

  private val product: Product = Await.result(execution.dbBinding.insert("perpetuo-app"), 1.second)

  private def getExecutions(dispatcher: TargetDispatcher): Future[Seq[(Long, Option[String])]] = {
    val req = new DeploymentRequestAttrs(product.name, Version("v42"), """"*"""", "No fear", "c.norris", new Timestamp(123456789))

    val depReq = execution.dbBinding.insert(req)
    val asyncStart = depReq.flatMap(execution.startOperation(dispatcher, _, Operation.deploy))
    asyncStart.flatMap { case (successes, failures) =>
      depReq.map(_.id).flatMap(execution.dbBinding.findExecutionTracesByDeploymentRequest).map { traces =>
        val executions = traces.map(trace => {
          (trace.id, trace.logHref)
        })
        successes shouldEqual executions.length
        failures shouldEqual 0
        executions
      }
    }
  }

  implicit class SimpleDispatchTest(private val select: Select) {
    def dispatchedAs(that: Iterable[(ExecutorInvoker, Select)]): Unit = {
      Set(TargetTerm(select = select)) dispatchedAs that.map { case (e, s) => (e, Set(TargetTerm(select = s))) }
    }
  }

  implicit class ComplexDispatchTest(private val target: TargetExpr) {
    private val rawTarget = target.toJson.compactPrint
    private val request = new DeploymentRequestAttrs(product.name, Version("v42"), rawTarget, "No fear", "c.norris", new Timestamp(123456789))
    private val depReq = execution.dbBinding.insert(request)

    def dispatchedAs(that: Iterable[(ExecutorInvoker, TargetExpr)]): Unit = {
      execution.dispatch(TestSuffixDispatcher, target) should contain theSameElementsAs that

      execLogs.clear()
      Await.result(
        depReq.flatMap(execution.startOperation(TestSuffixDispatcher, _, Operation.deploy).map(_ =>
          execLogs should contain theSameElementsAs that
        )),
        1.second
      )
    }
  }


  "A trivial execution" should {
    "trigger a job with no log href when there is no log href provided" in {
      Await.result(
        getExecutions(DummyTargetDispatcher).map(_ shouldEqual Seq((1, None))),
        1.second
      )
    }

    "trigger a job with a log href when a log href is provided as a Future" in {
      Await.result(
        getExecutions(SingleTargetDispatcher(DummyInvokerWithLogHref)).map(_ shouldEqual Seq((2, Some("#1")))),
        1.second
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
      val thrown = the[Exception] thrownBy execution.dispatch(TestSuffixDispatcher, Set(TargetTerm(select = Set("abc", "def"))))
      thrown.getMessage should endWith("is not covered by executors; can't operate.")
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
      )) should contain theSameElementsAs Map(
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
        barInvoker -> Alternatives(// there is only one possible representation for such a simple expression
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
      dispatchedTargets should contain theSameElementsAs Seq(
        Set(
          TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def", "ghi")),
          TargetTerm(Set(JsObject("foo" -> JsString("bar2")), JsObject("foo2" -> JsString("bar"))), Set("ghi"))
        )
      )
    }
  }
}
