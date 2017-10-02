package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Collection => JavaCollection, Map => JavaMap}

import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.model.{TargetAtom, Version}

import scala.collection.JavaConverters._


abstract class TargetDispatcher {
  final def expandTarget(productName: String, productVersion: Version, target: TargetExpr): TargetExpr = {
    val toAtoms = (word: String) => {
      val atoms = fromTargetWordToAtoms(productName, productVersion.toString, word).asScala
      require(atoms.nonEmpty, s"Target word `$word` doesn't resolve to any atomic target")
      atoms.foreach(atom => require(atom.length <= TargetAtom.maxSize, s"Target `$atom` is too long"))
      atoms
    }
    target.map(term => TargetTerm(term.tactics, term.select.flatMap(toAtoms)))
  }

  final def dispatchToExecutors(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)] = {
    val dispatched = collectionAsScalaIterableConverter(
      dispatch(asJavaIterableConverter(targetAtoms).asJava, frozenParameters).entrySet
    ).asScala.map { entry => (entry.getKey, entry.getValue.asScala.toSet) }

    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    val dispatchedTargets = dispatched.toStream.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet
    assert(dispatchedTargets.subsetOf(targetAtoms),
      "More targets after dispatching than before: " + (dispatchedTargets -- targetAtoms).map(_.toString).mkString(", "))
    require(dispatchedTargets.size == targetAtoms.size,
      "Some target atoms have no designated executors: " + (targetAtoms -- dispatchedTargets).map(_.toString).mkString(", "))

    dispatched
  }

  protected def fromTargetWordToAtoms(productName: String, productVersion: String, targetWord: String): JavaCollection[String] =
    Seq(targetWord).asJava

  /**
    * `freezeParameters` must return the execution parameters serialized as they will be
    * provided to `trigger` in order to play or replay an execution in a deterministic way,
    * except that it must be replayable with a subset of the original target (so the targets
    * should not be included in the frozen parameters).
    * If the input doesn't make sense (the parameters are incompatible with each other),
    * it must return an `UnprocessableIntent` error, whose message will be displayed to the end user.
    */
  def freezeParameters(executionKind: String, productName: String, version: Version): String = ""

  protected def dispatch(targetAtoms: JavaIterable[String], frozenParameters: String): JavaMap[ExecutorInvoker, JavaIterable[String]]
}
