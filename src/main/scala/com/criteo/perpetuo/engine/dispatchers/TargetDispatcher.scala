package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.engine.{Provider, Select, TargetExpr, TargetTerm, UnprocessableIntent}
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
    def groupOn2[A, B](it: Iterable[(A, B)]): Map[B, Set[A]] =
      it.groupBy(_._2).map { case (k, v) => (k, v.map(_._1).toSet) }

    val perSelectAtom = groupOn2(
      expandedTarget.toStream.flatMap { case TargetTerm(tactics, select) =>
        select.toStream.flatMap(selectAtom =>
          tactics.map(tactic => (tactic, selectAtom)))
      }
    )

    // infer only once for all unique targets the executors required for each target word
    dispatchToExecutors(perSelectAtom.keySet, frozenParameters).map { case (executor, select) =>
      val atomsAndTactics = select.toStream.map(selectAtom => (selectAtom, perSelectAtom(selectAtom)))
      val flattened = atomsAndTactics.flatMap { case (selectAtom, tactics) =>
        tactics.toStream.map(tactic => (selectAtom, tactic))
      }
      val alternatives = Seq(
        // either group first by "select" atoms then group these atoms by common "tactics"
        groupOn2(atomsAndTactics).map(TargetTerm.tupled),
        // or group first by tactic then group the tactics by common "select" atoms
        groupOn2(groupOn2(flattened)).map(_.swap).map(TargetTerm.tupled)
      )
      (executor, alternatives.map(_.toSet).toSet)
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
      throw UnprocessableIntent("Unknown target(s): " + (targetAtoms -- flattened).map(_.toString).mkString(", "))

    dispatched
  }
}


class NoAvailableExecutor extends RuntimeException
