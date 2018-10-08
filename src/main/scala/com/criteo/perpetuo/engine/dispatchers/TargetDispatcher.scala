package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.{Provider, TargetAtomSet, UnprocessableIntent}
import com.criteo.perpetuo.model._


trait ParameterFreezer {
  /**
    * @return the execution parameters serialized as they will be provided
    *         to `dispatch` in order to play or replay an execution in a deterministic way,
    *         except that it must be replayable with a subset of the original target (so the targets
    *         should not be included in the frozen parameters).
    *         If the input doesn't make sense (the parameters are incompatible with each other),
    *         it must return an `UnprocessableIntent` error, whose message will be displayed to the end user.
    */
  def freezeParameters(productName: String, version: Version): String
}


trait TargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer {
  def get: TargetDispatcher = this

  /**
    * @return all the provided target atoms grouped by their dedicated executors
    * @throws NoAvailableExecutor if a target could not be dispatched
    */
  protected def dispatch(targetAtoms: TargetAtomSet, frozenParameters: String): Iterable[(ExecutionTrigger, TargetAtomSet)]

  final def dispatchAtoms(targetAtoms: TargetAtomSet, frozenParameters: String): Iterable[(ExecutionTrigger, TargetAtomSet)] = {
    val dispatched = dispatch(targetAtoms, frozenParameters).filter { case (_, subSet) => subSet.items.nonEmpty }

    val orderedAtoms = targetAtoms
      .items
      .toSeq
      .sortBy(_.name)
    val orderedDispatchedAtoms = dispatched
      .toSeq
      .flatMap { case (_, subSet) => subSet.items }
      .sortBy(_.name)

    if (orderedDispatchedAtoms.size < targetAtoms.items.size) {
      val diff = (targetAtoms.items -- orderedDispatchedAtoms).mkString(", ")
      throw UnprocessableIntent(s"No executor associated to some target(s): $diff")
    }

    if (orderedDispatchedAtoms != orderedAtoms) {
      val expected = orderedAtoms.mkString(", ")
      val got = orderedDispatchedAtoms.mkString(", ")
      throw new RuntimeException(s"Wrong partition of atoms: `$expected` has been dispatched as `$got`")
    }

    dispatched
  }
}


class NoAvailableExecutor extends RuntimeException
