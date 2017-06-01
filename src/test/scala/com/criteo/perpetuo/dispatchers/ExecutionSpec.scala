package com.criteo.perpetuo.dispatchers

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Operation, Product, Version}
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, _}

import scala.collection.JavaConverters._
import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}
import scala.collection.immutable.Stream
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


object TestTargetDispatcher extends TargetDispatcher {
  val aInvoker = new DummyInvoker("A's invoker")
  val bInvoker = new DummyInvoker("B's invoker")
  val cInvoker = new DummyInvoker("C's invoker")

  override protected def fromTargetWordToAtoms(productName: String, productVersion: String, targetWord: String): java.lang.Iterable[String] = {
    // the atomic targets are the input word split on dashes
    java.util.Arrays.stream(targetWord.split("-")).iterator.asScala.filter(!_.isEmpty).toSeq.asJava
  }

  def assign(targetAtom: String): java.lang.Iterable[ExecutorInvoker] = {
    // associate executors to target words wrt the latter's characters
    val invokers: Set[ExecutorInvoker] = targetAtom.flatMap {
      case 'a' => Some(aInvoker)
      case 'b' => Some(bInvoker)
      case 'c' => Some(cInvoker)
      case _ => None
    }.toSet
    invokers.asJava
  }
}


class ExecutionSpec extends Test with TestDb {

  import TestTargetDispatcher._

  private val dummyCounter = Stream.from(1).toIterator
  private val execLogs: ConcurrentMap[ExecutorInvoker, TargetExpr] = new TrieMap()
  private val execution = new Execution(new DbBinding(dbContext)) {
    override protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, target: TargetExpr): Unit = {
      execLogs.put(executor, target).map(prev => fail(s"Logs say the executor has ${target.toJson.compactPrint} to do, but it already has $prev to do!"))
    }
  }

  object DummyInvokerWithLogHref extends DummyInvoker("DummyWithLogHref") {
    override def trigger(operationName: String, executionId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
      super.trigger(operationName, executionId, productName, version, target, initiator).map { logHref =>
        assert(logHref.isEmpty)
        Some(s"#${dummyCounter.next}")
      }
    }
  }

  private val product: Product = Await.result(execution.dbBinding.insert("perpetuo-app"), 1.second)

  private def getExecutions(dispatcher: TargetDispatcher): Future[Seq[(Long, Option[String])]] = {
    val req = new DeploymentRequestAttrs(product.name, Version("v42"), """"*"""", "No fear", "c.norris", new Timestamp(123456789))

    val depReq = execution.dbBinding.insert(req)
    val asyncStart = depReq.flatMap(execution.startOperation(dispatcher, _, Operation.deploy, "c.norris"))
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
      execLogs.clear()
      Await.result(
        depReq.flatMap(execution.startOperation(TestTargetDispatcher, _, Operation.deploy, "c.norris").map(_ =>
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
      Set("a", "c") dispatchedAs Map(
        aInvoker -> Set("a"),
        cInvoker -> Set("c")
      )
    }

    "gather target words on executors" in {
      Set("abc-ab", "cb") dispatchedAs Map(
        aInvoker -> Set("abc", "ab"),
        bInvoker -> Set("abc", "ab", "cb"),
        cInvoker -> Set("abc", "cb")
      )
    }

    "raise if a target cannot be solved to atomic targets" in {
      val thrown = the[IllegalArgumentException] thrownBy TestTargetDispatcher.expandTarget(null, Version(""), Set(TargetTerm(select = Set("ab", "-"))))
      thrown.getMessage should endWith("Target word `-` doesn't resolve to any atomic target")
    }

    "raise if a target is not fully covered by executors" in {
      val thrown = the[IllegalArgumentException] thrownBy TestTargetDispatcher.dispatchToExecutors("def")
      thrown.getMessage should endWith("There is no executor for `def`")
    }

  }


  "A complex target expression" should {

    "be appropriately dispatched among impacted executors" in {
      Set(
        TargetTerm(select = Set("aa", "ab-a")),
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
          Set("bb-ab", "cc")
        )
      ) dispatchedAs Map(
        aInvoker -> Set(
          TargetTerm(select = Set("aa", "a")),
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
            Set("ab")
          )
        ),
        bInvoker -> Set(
          TargetTerm(select = Set("ab")),
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
            Set("bb", "ab")
          )
        ),
        cInvoker -> Set(
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
            Set("cc")
          )
        )
      )
    }

    "be dispatched as short target expressions" in {
      val Alternatives = Set
      execution.dispatchAlternatives(TestTargetDispatcher, Set(
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("ab")),
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("assault")),
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("appendix")),
        TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("alpha")),
        TargetTerm(select = Set("alpha"))
      )) should contain theSameElementsAs Map(
        aInvoker -> Alternatives(
          Set(
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("ab")),
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("assault", "appendix", "alpha")),
            TargetTerm(select = Set("alpha"))
          ),
          Set(
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("ab")),
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("assault", "appendix")),
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05)), JsObject()), Set("alpha"))
          )
        ),
        bInvoker -> Alternatives( // there is only one possible representation for such a simple expression
          Set(
            TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("ab"))
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
