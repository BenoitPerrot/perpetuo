package com.criteo.perpetuo.engine

import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.{ExecutionTrigger, NoOpTrigger}
import com.criteo.perpetuo.model._
import com.criteo.perpetuo.{SimpleScenarioTesting, TestTargetResolver}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.TraversableOnce
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.reflect.{ClassTag, classTag}


object TestTargetDispatcher extends TargetDispatcher {
  val aTrigger = new NoOpTrigger
  val bTrigger = new NoOpTrigger
  val cTrigger = new NoOpTrigger

  override def freezeParameters(productName: String, version: Version): String = "foobar"

  protected override def dispatch(targetAtoms: Set[TargetAtom], frozenParameters: String): Iterable[(ExecutionTrigger, Set[TargetAtom])] = {
    assert(frozenParameters == "foobar")
    // associate executors to target words wrt the each target word's characters
    targetAtoms
      .flatMap(atom =>
        atom
          .name
          .flatMap {
            case 'a' => Some(aTrigger)
            case 'b' => Some(bTrigger)
            case 'c' => Some(cTrigger)
            case _ => None
          }
          .map(executor => (executor, atom))
      )
      .groupBy(_._1)
      .map { case (executor, it) => executor -> it.map(_._2) }
  }
}


class DispatchingSpec extends SimpleScenarioTesting {

  import TestTargetDispatcher._

  protected override def providesTargetDispatcher: TargetDispatcher = TestTargetDispatcher
  private val testResolver = TestTargetResolver
  private val product: Product = Await.result(crankshaft.dbBinding.upsertProduct("perpetuo-app"), 1.second)

  implicit class DispatchTest(private val target: Set[String]) {
    private val request = ProtoDeploymentRequest(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", target.toJson, "")), "No fear", "c.norris")

    def dispatchedAs(that: Map[ExecutionTrigger, Set[String]]): Unit = {
      Await.result(
        crankshaft.dbBinding.insertDeploymentRequest(request)
          .flatMap { deploymentPlan =>
            val firstStep = deploymentPlan.steps.head
            val expandedTarget = testResolver.resolveExpression(firstStep.deploymentRequest.product.name, firstStep.deploymentRequest.version, firstStep.parsedTarget)
            crankshaft.dbBinding.dbContext.db.run(
              crankshaft.getStepSpecifics(expandedTarget, firstStep)
            )
          }
          .map { case (_, executionsToTrigger) =>
            val toTrigger = executionsToTrigger.flatMap { case (_, executionToTrigger) => executionToTrigger }
            assertEqual(toTrigger.toMap.mapValues(_.superset.map(_.name)), that)
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

  test("An execution cannot distribute an atom to multiple executors") {
    val params = TestTargetDispatcher.freezeParameters("", Version(""""42""""))
    val thrown = the[RuntimeException] thrownBy TestTargetDispatcher.dispatchAtomSet(TargetAtomSet(Set.empty, Set("abc", "ab", "cb").map(TargetAtom)), params)
    thrown.getMessage shouldEqual "Wrong partition of atoms: `ab, abc, cb` has been dispatched as `ab, ab, abc, abc, abc, cb, cb`"
  }

  test("An execution raises if a target cannot be solved to atomic targets") {
    val targets = Set(TargetAtom("ab"), TargetTag("-"))
    val thrown = the[UnprocessableIntent] thrownBy testResolver.resolveExpression(null, Version("\"\""), TargetUnion(targets.toSet))
    thrown.getMessage shouldEqual "The following target(s) were not resolved: [-]"
  }

  test("The resolver cuts short on atoms and sends them back") {
    val targets = Set(TargetAtom("ab"), TargetAtom("-"))
    testResolver.resolveExpression(null, Version("\"\""), TargetUnion(targets.toSet)).superset shouldEqual targets
    // the fact it doesn't throw proves the shortcut (see test above)
  }

  test("An execution raises if a target is not fully covered by executors") {
    val params = TestTargetDispatcher.freezeParameters("", Version(""""42""""))
    val thrown = the[UnprocessableIntent] thrownBy TestTargetDispatcher.dispatchAtomSet(TargetAtomSet(Set.empty, Set(TargetAtom("def"))), params)
    thrown.getMessage shouldEqual "No executor associated to some target(s): def"
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
        challenger.getClass shouldEqual expected.getClass
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
