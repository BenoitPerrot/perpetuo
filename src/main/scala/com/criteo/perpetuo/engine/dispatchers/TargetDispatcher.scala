package com.criteo.perpetuo.engine.dispatchers

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

  final def dispatchToExecutors(targetAtom: String): Set[ExecutorInvoker] = {
    val executors = assign(targetAtom).asScala.toSet
    require(executors.nonEmpty, s"There is no executor for `$targetAtom`")
    executors
  }

  protected def fromTargetWordToAtoms(productName: String, productVersion: String, targetWord: String): java.lang.Iterable[String] =
    Seq(targetWord).asJava

  /**
    * `freezeParameters` must return the execution parameters serialized as they will be
    * provided to `trigger` in order to play or replay an execution in a deterministic way,
    * except that it must be replayable with a subset of the original target (so the targets
    * should not be included in the frozen parameters).
    * If the input doesn't make sense (the parameters are incompatible with each other),
    * it must return a `UnprocessableIntent` error, whose message will be displayed to the end user.
    */
  def freezeParameters(executionKind: String, productName: String, version: Version): String = ""

  // todo: rename as `dispatch` or `toExecutors`
  protected def assign(targetAtom: String): java.lang.Iterable[ExecutorInvoker]
}
