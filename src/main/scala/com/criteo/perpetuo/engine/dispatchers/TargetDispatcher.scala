package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.{Provider, Select, TargetExpr, UnprocessableIntent}
import com.criteo.perpetuo.model.Version
import spray.json.DefaultJsonProtocol._
import spray.json._


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
  def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutionTrigger, Select)]

  def dispatchExpression(expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] =
    dispatchAlternatives(expandedTarget, frozenParameters).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.minBy(_.toJson.compactPrint.length))
    }

  private[engine] def dispatchAlternatives(expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, Set[TargetExpr])] = {
    // infer only once for all unique targets the executors required for each target word
    dispatchToExecutors(expandedTarget, frozenParameters).map { case (executor, select) =>
      (executor, Set(select)) // fixme: change the return type to reflect that there is only one select now
    }
  }

  private[engine] def dispatchToExecutors(targetAtoms: Select, frozenParameters: String) = {
    val dispatched = dispatch(targetAtoms, frozenParameters)
      .toStream
      .filter { case (_, select) => select.nonEmpty }

    val flattened: Select = dispatched.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet

    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    if (!flattened.subsetOf(targetAtoms))
      throw new RuntimeException("The dispatcher augmented the original intent, which is forbidden. The targets introduced after dispatching are: " +
        (flattened -- targetAtoms).map(_.toString).mkString(", "))
    if (flattened.size != targetAtoms.size)
      throw UnprocessableIntent("The following target(s) were not resolved: " +
        (targetAtoms -- flattened).map(_.toString).mkString(", "))

    dispatched
  }
}


class NoAvailableExecutor extends RuntimeException
