package com.criteo.perpetuo.engine

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.invokers.{DummyInvoker, ExecutorInvoker}
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Operation, Product, Version}
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.TraversableOnce
import scala.collection.concurrent.{TrieMap, Map => ConcurrentMap}
import scala.collection.immutable.Stream
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.{ClassTag, classTag}


object TestTargetDispatcher extends TargetDispatcher {
  val aInvoker = new DummyInvoker("A's invoker")
  val bInvoker = new DummyInvoker("B's invoker")
  val cInvoker = new DummyInvoker("C's invoker")

  override def freezeParameters(executionKind: String, productName: String, version: Version) = "foobar"

  override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)] = {
    assert(frozenParameters == "foobar")
    // associate executors to target words wrt the each target word's characters
    targetAtoms.flatMap { targetAtom =>
      targetAtom.flatMap {
        case 'a' => Some(aInvoker)
        case 'b' => Some(bInvoker)
        case 'c' => Some(cInvoker)
        case _ => None
      }.map(executor => (executor, targetAtom))
    }.groupBy(_._1).map { case (executor, it) => executor.asInstanceOf[ExecutorInvoker] -> it.map(_._2) }
  }
}

object DummyTargetDispatcher extends SingleTargetDispatcher(executorInvoker = new DummyInvoker("Default Dummy Invoker"))

class OperationStarterSpec extends Test with TestDb {

  import TestTargetDispatcher._

  private val dummyCounter = Stream.from(1).toIterator
  private val execLogs: ConcurrentMap[ExecutorInvoker, TargetExpr] = new TrieMap()
  private val execution = new OperationStarter(new DbBinding(dbContext)) {
    override protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, target: TargetExpr): Unit = {
      execLogs.put(executor, target).map(prev => fail(s"Logs say the executor has ${target.toJson.compactPrint} to do, but it already has $prev to do!"))
    }
  }
  private val testResolver = new TargetResolver {
    override def toAtoms(productName: String, productVersion: String, targetWords: Select): Map[String, Select] = {
      // the atomic targets are the input word split on dashes
      targetWords.map(word => word -> word.split("-").filter(_.nonEmpty).toSet).toMap
    }
  }

  object DummyInvokerWithLogHref extends DummyInvoker("DummyWithLogHref") {
    override def trigger(execTraceId: Long, executionKind: String, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
      super.trigger(execTraceId, executionKind, productName, version, target, initiator).map { logHref =>
        assert(logHref.isEmpty)
        Some(s"#${dummyCounter.next}")
      }
    }
  }

  private val product: Product = Await.result(execution.dbBinding.insertProduct("perpetuo-app"), 1.second)

  private def getExecutions(dispatcher: TargetDispatcher): Future[Seq[(Long, Option[String])]] = {
    val req = new DeploymentRequestAttrs(product.name, Version("\"v42\""), """"*"""", "No fear", "c.norris", new Timestamp(123456789))

    val depReq = execution.dbBinding.insertDeploymentRequest(req)
    val asyncStart = depReq.flatMap(execution.start(testResolver, dispatcher, _, Operation.deploy, "c.norris"))
    asyncStart.flatMap { case (_, successes, failures) =>
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
    def dispatchedAs(that: Map[ExecutorInvoker, Select]): Unit = {
      Set(TargetTerm(select = select)) dispatchedAs that.map { case (e, s) => (e, Set(TargetTerm(select = s))) }
    }
  }

  implicit class ComplexDispatchTest(private val target: TargetExpr) {
    private val rawTarget = target.toJson.compactPrint
    private val request = new DeploymentRequestAttrs(product.name, Version("\"v42\""), rawTarget, "No fear", "c.norris", new Timestamp(123456789))
    private val depReq = execution.dbBinding.insertDeploymentRequest(request)

    def dispatchedAs(that: Map[ExecutorInvoker, TargetExpr]): Unit = {
      execLogs.clear()
      Await.result(
        depReq.flatMap(execution.start(testResolver, TestTargetDispatcher, _, Operation.deploy, "c.norris").map(_ =>
          assertEqual(execLogs, that)
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
      val thrown = the[IllegalArgumentException] thrownBy execution.expandTarget(testResolver, null, Version("\"\""), Set(TargetTerm(select = Set("ab", "-"))))
      thrown.getMessage should endWith("Target word `-` doesn't resolve to any atomic target")
    }

    "raise if a target is not fully covered by executors" in {
      val params = TestTargetDispatcher.freezeParameters("", "", Version(""""42""""))
      val thrown = the[IllegalArgumentException] thrownBy execution.dispatchToExecutors(TestTargetDispatcher, Set("def"), params)
      thrown.getMessage shouldEqual "requirement failed: Some target atoms have been lost in dispatching: def"
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
      val params = TestTargetDispatcher.freezeParameters("", "", Version(""""42""""))
      assertEqual(
        execution.dispatchAlternatives(TestTargetDispatcher, Set(
          TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05), "foo" -> JsString("bar"))), Set("ab")),
          TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("assault")),
          TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("appendix")),
          TargetTerm(Set(JsObject("ratio" -> JsNumber(0.05))), Set("alpha")),
          TargetTerm(select = Set("alpha"))
        ), params).toMap,
        Map(
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
        .dispatch(DummyTargetDispatcher, targetWithDuplicates, "")
        .map(_._2)
      assertEqual(dispatchedTargets, Seq(
        Set(
          TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def", "ghi")),
          TargetTerm(Set(JsObject("foo" -> JsString("bar2")), JsObject("foo2" -> JsString("bar"))), Set("ghi"))
        )
      ))
    }
  }

  private def assertEqual(challenger: Any, expected: Any, path: String = "root"): Unit = {
    challenger match {
      case cMap: scala.collection.Map[_, _] =>
        val eMap = as[scala.collection.Map[Any, Any]](expected, path)
        assertEqualSets(cMap.asInstanceOf[scala.collection.Map[Any, Any]].keySet, eMap.keySet, path)
        cMap.foreach { case (k, v) => assertEqual(v, eMap(k), s"$path/$k") }
      case cSet: scala.collection.Set[_] =>
        assertEqualSets(cSet.asInstanceOf[scala.collection.Set[Any]], as[scala.collection.Set[Any]](expected, path), path)
      case cTuple: scala.Product =>
        assertEqual(cTuple.productIterator, as[scala.Product](expected, path).productIterator, path)
      case cIt: TraversableOnce[Any] =>
        val lc = cIt.toSeq
        val le = as[TraversableOnce[Any]](expected, path).toSeq
        val common = lc.zip(le).zipWithIndex.takeWhile { case ((c, e), i) =>
          assertEqual(c, e, s"$path[$i]")
          true
        }.size
        val cSuffix = lc.drop(common)
        val eSuffix = le.drop(common)
        Predef.assert(cSuffix.isEmpty, s"Unexpected elements in the iterable at $path from element $common: ${cSuffix.mkString(", ")}")
        Predef.assert(eSuffix.isEmpty, s"Missing elements in the iterable at $path from element $common: ${eSuffix.mkString(", ")}")
      case _ =>
        Predef.assert(challenger == expected, s"Expected $expected, found $challenger at $path")
    }
  }

  private def as[T: ClassTag](c: Any, path: String): T = {
    assert(classTag[T].runtimeClass.isInstance(c), s"Expected a ${classTag[T].runtimeClass.getName} at $path, got $c (of type ${c.getClass.getName})")
    c.asInstanceOf[T]
  }

  private def assertEqualSets(l: scala.collection.Set[Any], r: scala.collection.Set[Any], path: String): Unit = {
    Predef.assert(l == r,
      if (l.diff(r).nonEmpty) s"Unexpected element(s) in $path: ${l.diff(r).mkString(", ")}"
      else s"Missing element(s) in $path: ${r.diff(l).mkString(", ")}"
    )
  }
}
