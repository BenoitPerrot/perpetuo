package com.criteo.perpetuo.engine

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.engine.dispatchers.{SingleTargetDispatcher, TargetDispatcher}
import com.criteo.perpetuo.engine.invokers.{DummyUnstoppableInvoker, ExecutorInvoker}
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model._
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.TraversableOnce
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}


object TestTargetDispatcher extends TargetDispatcher {
  val aInvoker = new DummyUnstoppableInvoker("A's invoker")
  val bInvoker = new DummyUnstoppableInvoker("B's invoker")
  val cInvoker = new DummyUnstoppableInvoker("C's invoker")

  override def freezeParameters(productName: String, version: Version): String = "foobar"

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

object DummyTargetDispatcher extends SingleTargetDispatcher(executorInvoker = new DummyUnstoppableInvoker("Default Dummy Invoker"))

class OperationStarterSpec extends Test with TestDb {

  import TestTargetDispatcher._

  private val operationStarter = new OperationStarter(new DbBinding(dbContext))
  private val testResolver = new TargetResolver {
    override def toAtoms(productName: String, productVersion: Version, targetWords: Select): Option[Map[String, Select]] = {
      // the atomic targets are the input word split on dashes
      Some(targetWords.map(word => word -> word.split("-").filter(_.nonEmpty).toSet).toMap)
    }
  }

  private val product: Product = Await.result(operationStarter.dbBinding.insertProduct("perpetuo-app"), 1.second)

  implicit class SimpleDispatchTest(private val select: Select) {
    def dispatchedAs(that: Map[ExecutorInvoker, Select]): Unit = {
      Set(TargetTerm(select = select)) dispatchedAs that.map { case (e, s) => (e, Set(TargetTerm(select = s))) }
    }
  }

  implicit class ComplexDispatchTest(private val target: TargetExpr) {
    private val rawTarget = target.toJson.compactPrint
    private val request = new DeploymentRequestAttrs(product.name, Version("\"v42\""), rawTarget, "No fear", "c.norris", new Timestamp(123456789))
    private val depReq = operationStarter.dbBinding.insertDeploymentRequest(request)

    def dispatchedAs(that: Map[ExecutorInvoker, TargetExpr]): Unit = {
      Await.result(
        depReq.flatMap(
          operationStarter.startDeploymentRequest(testResolver, TestTargetDispatcher, _, "c.norris")
            .map(_._1)
            .flatMap(dbContext.db.run)
            .map { case (_, toTrigger) =>
              assertEqual(toTrigger.map { case (_, _, targetExpr, invoker) => (invoker, targetExpr) }.toMap, that)
              assertEqual(toTrigger.size, that.size) // look for unexpected duplicates
            }
        ),
        1.second
      )
    }
  }


  test("A complex execution calls the right executor when available for each exact target word") {
    Set("a", "c") dispatchedAs Map(
      aInvoker -> Set("a"),
      cInvoker -> Set("c")
    )
  }

  test("A complex execution gathers target words on executors") {
    Set("abc-ab", "cb") dispatchedAs Map(
      aInvoker -> Set("abc", "ab"),
      bInvoker -> Set("abc", "ab", "cb"),
      cInvoker -> Set("abc", "cb")
    )
  }

  test("A complex execution raises if a target cannot be solved to atomic targets") {
    val thrown = the[IllegalArgumentException] thrownBy operationStarter.expandTarget(testResolver, null, Version("\"\""), Set(TargetTerm(select = Set("ab", "-"))))
    thrown.getMessage should endWith("`-` is not a valid target in that context")
  }

  test("A complex execution raises if a target is not fully covered by executors") {
    val params = TestTargetDispatcher.freezeParameters("", Version(""""42""""))
    val thrown = the[IllegalArgumentException] thrownBy operationStarter.dispatchToExecutors(TestTargetDispatcher, Set("def"), params)
    thrown.getMessage shouldEqual "requirement failed: Some targets have been lost in dispatching: def"
  }


  test("A complex target expression is appropriately dispatched among impacted executors") {
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

  test("A complex target expression is dispatched as short target expressions") {
    val Alternatives = Set
    val params = TestTargetDispatcher.freezeParameters("", Version(""""42""""))
    assertEqual(
      operationStarter.dispatchAlternatives(TestTargetDispatcher, Set(
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


  test("Target dispatching deduplicates targets and regenerate them (per executor) in a concise way") {
    val targetWithDuplicates: TargetExpr = Set(
      TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def")),
      TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def")),
      TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc")),
      TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("def")),
      TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("ghi")),
      TargetTerm(Set(JsObject("foo" -> JsString("bar2"))), Set("ghi")),
      TargetTerm(Set(JsObject("foo2" -> JsString("bar"))), Set("ghi"))
    )
    val dispatchedTargets = operationStarter
      .dispatch(DummyTargetDispatcher, targetWithDuplicates, "")
      .map(_._2)
    assertEqual(dispatchedTargets, Seq(
      Set(
        TargetTerm(Set(JsObject("foo" -> JsString("bar"), "bar" -> JsString("baz"))), Set("abc", "def", "ghi")),
        TargetTerm(Set(JsObject("foo" -> JsString("bar2")), JsObject("foo2" -> JsString("bar"))), Set("ghi"))
      )
    ))
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
        assert(cSuffix.isEmpty, s"Unexpected elements in the iterable at $path from element $common: ${cSuffix.mkString(", ")}")
        assert(eSuffix.isEmpty, s"Missing elements in the iterable at $path from element $common: ${eSuffix.mkString(", ")}")
      case _ =>
        assert(challenger == expected, s"Expected $expected, found $challenger at $path")
    }
  }

  private def as[T: ClassTag](c: Any, path: String): T = {
    assert(classTag[T].runtimeClass.isInstance(c), s"Expected a ${classTag[T].runtimeClass.getName} at $path, got $c (of type ${c.getClass.getName})")
    c.asInstanceOf[T]
  }

  private def assertEqualSets(l: scala.collection.Set[Any], r: scala.collection.Set[Any], path: String): Unit = {
    assert(l == r,
      if (l.diff(r).nonEmpty) s"Unexpected element(s) in $path: ${l.diff(r).mkString(", ")}"
      else s"Missing element(s) in $path: ${r.diff(l).mkString(", ")}"
    )
  }
}
