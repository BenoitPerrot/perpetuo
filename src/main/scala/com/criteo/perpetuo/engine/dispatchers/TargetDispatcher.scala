package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.{Provider, UnprocessableIntent}
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
  protected def dispatch(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)]

  final def dispatchExpression(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] = {
    val dispatched = dispatch(targetExpr, frozenParameters).filter { case (_, subExpr) => subExpr.nonEmpty }

    val partition = targetExpr match {
      case a: TargetAtom => Some(Seq(a))
      case TargetAtomSet(atoms) => Some(atoms.toSeq.sortBy(_.name))
      case _ => None
    }
    partition
      .orElse {
        if (dispatched.forall { case (_, e) => e.isEmpty })
          throw UnprocessableIntent(s"No executor associated to the target `$targetExpr`")
        if (dispatched.size != 1 || dispatched.head._2 != targetExpr)
          throw new RuntimeException(s"Dispatching unresolved expression `$targetExpr` to: ${dispatched.map(_._2).mkString(", ")}")
        None
      }
      .foreach { atoms =>
        val dispatchedAtoms = dispatched
          .toSeq
          .flatMap { case (_, subExpr) =>
            subExpr match {
              case a: TargetAtom => Seq(a)
              case TargetUnion(items) => items.map {
                case a: TargetAtom => a
                case _ => throw new RuntimeException(s"Wrong dispatching: unexpected sub-expression `$subExpr` generated out of `$targetExpr`")
              }
              case TargetAtomSet(items) => items
              case _ => throw new RuntimeException(s"Wrong dispatching: unexpected sub-expression `$subExpr` generated out of `$targetExpr`")
            }
          }
          .sortBy(_.name)
        if (dispatchedAtoms != atoms)
          throw new RuntimeException(s"Wrong partition of atoms: `${atoms.mkString(", ")}` has been dispatched as `${dispatchedAtoms.mkString(", ")}`")
      }

    dispatched
  }
}


class NoAvailableExecutor extends RuntimeException
