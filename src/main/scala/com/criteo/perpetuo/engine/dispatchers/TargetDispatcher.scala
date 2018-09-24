package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.{Provider, UnprocessableIntent}
import com.criteo.perpetuo.model.{TargetAtomSet, TargetExpr, TargetTerm, TargetUnion, Version}


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
    val dispatched = dispatch(targetExpr, frozenParameters).filter { case (_, subExpr) => subExpr.nonEmpty }

    if (dispatched.size != 1 || dispatched.head._2 != targetExpr) { // cut short in the case of a trivial dispatcher
      val normalizedBefore = Dispatch.normalizeExpr(targetExpr)
      val normalizedAfter = dispatched
        .map { case (_, group) => Dispatch.normalizeExpr(group) }
        .reduceOption(_ ++ _) // instead of fold() to not build from an empty set
        .getOrElse(Set())

      // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
      if (!normalizedAfter.subsetOf(normalizedBefore))
        throw new RuntimeException("The dispatcher augmented the original intent, which is forbidden. The targets introduced after dispatching are: " +
          (normalizedAfter -- normalizedBefore).mkString(", "))
      if (normalizedAfter.size != normalizedBefore.size)
        throw UnprocessableIntent("The following target(s) were not dispatched: " +
          (normalizedBefore -- normalizedAfter).mkString(", "))
    }

    dispatched
  }
}


// fixme: temporarily, while we go from untyped to typed and back to untyped expression (migration)!
object Dispatch {
  val normalizeExpr: TargetExpr => Set[TargetTerm] = {
    case t: TargetTerm => Set(t)
    case TargetUnion(items) => items.flatMap(normalizeExpr)
    case TargetAtomSet(items) => items.toSet
  }
}


class NoAvailableExecutor extends RuntimeException
