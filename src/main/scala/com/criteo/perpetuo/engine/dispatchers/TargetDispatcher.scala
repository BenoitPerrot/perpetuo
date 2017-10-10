package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Collection => JavaCollection, Map => JavaMap}

import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.model.Version

import scala.collection.JavaConverters._


class TargetResolver {
  /**
    * A given target word can express the union of multiple targets; resolving a target consists
    * in computing the list of all the underlying targets (e.g. the European Union
    * COULD be "resolved" in this context as the list of all the names of its member states).
    * An atomic target is a target that cannot be resolved to smaller parts of itself because
    * FOR THE GIVEN PRODUCT, IT WILL NEVER BE POSSIBLE TO DEPLOY TO A SUBSET OF THAT TARGET.
    * If you can choose between Paris and Lyon as a target in France, but nothing finer, then
    * the result of this function `toAtoms` for the input "European Union" CANNOT contain "France"
    * but MUST contain at least "Paris" and "Lyon".
    * For a given atomic target (let's say "Lyon"), this function must return the input itself as
    * only element in the list (i.e. "Lyon" resolves to Iterable("Lyon")).
    *
    * @return the atomic target words to which the input target word is resolved wrt the given product and version.
    */
  def toAtoms(productName: String, productVersion: String, targetWord: String): JavaCollection[String] =
    Seq(targetWord).asJava
}


abstract class TargetDispatcher {
  final def dispatchToExecutors(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)] = {
    val dispatched = collectionAsScalaIterableConverter(
      dispatch(asJavaIterableConverter(targetAtoms).asJava, frozenParameters).entrySet
    ).asScala
      .map { entry => (entry.getKey, entry.getValue.asScala.toSet) }
      .filter { case (_, select) => select.nonEmpty }

    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    val dispatchedTargets = dispatched.toStream.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet
    assert(dispatchedTargets.subsetOf(targetAtoms),
      "More targets after dispatching than before: " + (dispatchedTargets -- targetAtoms).map(_.toString).mkString(", "))
    require(dispatchedTargets.size == targetAtoms.size,
      "Some target atoms have no designated executors: " + (targetAtoms -- dispatchedTargets).map(_.toString).mkString(", "))

    dispatched
  }

  /**
    * @return the execution parameters serialized as they will be provided
    *         to `dispatch` in order to play or replay an execution in a deterministic way,
    *         except that it must be replayable with a subset of the original target (so the targets
    *         should not be included in the frozen parameters).
    *         If the input doesn't make sense (the parameters are incompatible with each other),
    *         it must return an `UnprocessableIntent` error, whose message will be displayed to the end user.
    */
  def freezeParameters(executionKind: String, productName: String, version: Version): String

  /**
    * @return all the provided target atoms grouped by their dedicated executors
    */
  protected def dispatch(targetAtoms: JavaIterable[String], frozenParameters: String): JavaMap[ExecutorInvoker, JavaIterable[String]]
}
