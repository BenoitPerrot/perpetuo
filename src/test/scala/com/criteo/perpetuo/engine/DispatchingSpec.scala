package com.criteo.perpetuo.engine

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.{DummyExecutionTrigger, ExecutionTrigger}
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.TraversableOnce
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}


object TestTargetDispatcher extends TargetDispatcher {
  val aTrigger = new DummyExecutionTrigger("A's trigger")
  val bTrigger = new DummyExecutionTrigger("B's trigger")
  val cTrigger = new DummyExecutionTrigger("C's trigger")

  override def freezeParameters(productName: String, version: Version): String = "foobar"

  override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutionTrigger, Select)] = {
    assert(frozenParameters == "foobar")
    // associate executors to target words wrt the each target word's characters
    targetAtoms.flatMap { targetAtom =>
      targetAtom.flatMap {
        case 'a' => Some(aTrigger)
        case 'b' => Some(bTrigger)
        case 'c' => Some(cTrigger)
        case _ => None
      }.map(executor => (executor, targetAtom))
    }.groupBy(_._1).map { case (executor, it) => executor.asInstanceOf[ExecutionTrigger] -> it.map(_._2) }
  }
}


class DispatchingSpec extends SimpleScenarioTesting {

  import TestTargetDispatcher._

  override lazy val targetDispatcher: TargetDispatcher = TestTargetDispatcher

  private val testResolver = new TargetResolver {
    override def toAtoms(productName: String, productVersion: Version, targetWords: Select): Option[Map[String, Select]] = {
      // the atomic targets are the input word split on dashes
      Some(targetWords.map(word => word -> word.split("-").filter(_.nonEmpty).toSet).toMap)
    }
  }

  private val product: Product = Await.result(crankshaft.dbBinding.upsertProduct("perpetuo-app"), 1.second)

  implicit class DispatchTest(private val target: TargetExpr) {
    private val request = ProtoDeploymentRequest(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", target.toJson, "")), "No fear", "c.norris")

    def dispatchedAs(that: Map[ExecutionTrigger, TargetExpr]): Unit = {
      Await.result(
        crankshaft.dbBinding.insertDeploymentRequest(request)
          .flatMap { deploymentPlan =>
            val firstStep = deploymentPlan.steps.head
            val expandedTarget = testResolver.resolveExpression(firstStep.deploymentRequest.product.name, firstStep.deploymentRequest.version, firstStep.parsedTarget)
            crankshaft.dbBinding.dbContext.db.run(
              crankshaft.getStepSpecifics(expandedTarget, firstStep)
            )
          }
          .map { case (_, executionsToTrigger, _) =>
            val toTrigger = executionsToTrigger.flatMap { case (_, executionToTrigger) => executionToTrigger }
            assertEqual(toTrigger.toMap, that)
            assertEqual(toTrigger.size, that.size) // look for unexpected duplicates
          },
        1.second
      )
    }
  }


  test("An execution calls the right executor when available for each exact target word") {
    Set("a", "c") dispatchedAs Map(
      aTrigger -> Set("a"),
      cTrigger -> Set("c")
    )
  }

  test("An execution gathers target words on executors") {
    Set("abc-ab", "cb") dispatchedAs Map(
      aTrigger -> Set("abc", "ab"),
      bTrigger -> Set("abc", "ab", "cb"),
      cTrigger -> Set("abc", "cb")
    )
  }

  test("An execution raises if a target cannot be solved to atomic targets") {
    val thrown = the[UnprocessableIntent] thrownBy testResolver.resolveExpression(null, Version("\"\""), Set("ab", "-"))
    thrown.getMessage shouldEqual "The following target(s) were not resolved: -"
  }

  test("An execution raises if a target is not fully covered by executors") {
    val params = TestTargetDispatcher.freezeParameters("", Version(""""42""""))
    val thrown = the[UnprocessableIntent] thrownBy TestTargetDispatcher.dispatchExpression(Set("def"), params)
    thrown.getMessage shouldEqual "The following target(s) were not resolved: def"
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