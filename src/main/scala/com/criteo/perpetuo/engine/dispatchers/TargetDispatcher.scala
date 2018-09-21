package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.{Provider, UnprocessableIntent}
import com.criteo.perpetuo.model.{TargetExpr, Version}


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
  def dispatch(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)]

  def dispatchExpression(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] = {
    val dispatched = dispatch(targetExpr, frozenParameters)
      .toStream
      .filter { case (_, select) => select.nonEmpty }

    val flattened: TargetExpr = dispatched.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet

    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    if (!flattened.subsetOf(targetExpr))
      throw new RuntimeException("The dispatcher augmented the original intent, which is forbidden. The targets introduced after dispatching are: " +
        (flattened -- targetExpr).map(_.toString).mkString(", "))
    if (flattened.size != targetExpr.size)
      throw UnprocessableIntent("The following target(s) were not dispatched: " +
        (targetExpr -- flattened).map(_.toString).mkString(", "))

    dispatched
  }
}


class NoAvailableExecutor extends RuntimeException
