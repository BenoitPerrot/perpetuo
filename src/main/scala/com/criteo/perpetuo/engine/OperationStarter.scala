package com.criteo.perpetuo.engine

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.engine.dispatchers.{TargetDispatcher, UnprocessableIntent}
import com.criteo.perpetuo.engine.invokers.ExecutorInvoker
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class UnprocessableAction(msg: String, detail: Map[String, _] = Map())
  extends RuntimeException(msg + detail.map {
    case (k, v: Iterable[_]) => s"; $k: ${v.mkString(", ")}"
    case (k, v) => s"; $k: $v"
  }.mkString(""))


class OperationStarter(val dbBinding: DbBinding) extends Logging {

  /**
    * Start all relevant executions and return the numbers of successful
    * and failed execution starts.
    */
  def start(resolver: TargetResolver, dispatcher: TargetDispatcher, deploymentRequest: DeepDeploymentRequest, operation: Operation.Kind, userName: String): Future[(OperationTrace, Int, Int)] = {
    // generation of specific parameters
    val specificParameters = dispatcher.freezeParameters(deploymentRequest.product.name, deploymentRequest.version)
    // target resolution
    val expandedTarget = expandTarget(resolver, deploymentRequest.product.name, deploymentRequest.version, deploymentRequest.parsedTarget)
    // execution dispatching
    val invocations = dispatch(dispatcher, expandedTarget, specificParameters).toSeq

    dbBinding.insertOperationTrace(deploymentRequest.id, operation, userName).flatMap(operationTrace =>
      dbBinding.insertExecutionSpecification(specificParameters, deploymentRequest.version).flatMap(executionSpecification =>
        startExecution(invocations, deploymentRequest, operationTrace, executionSpecification).map {
          case (successes, failures) => (operationTrace, successes, failures)
        }
      )
    )
  }

  def deployAgain(resolver: TargetResolver,
                  dispatcher: TargetDispatcher,
                  deploymentRequest: DeepDeploymentRequest,
                  executionSpecs: Seq[ExecutionSpecification],
                  userName: String): Future[(OperationTrace, Int, Int)] = {

    dbBinding.insertOperationTrace(deploymentRequest.id, Operation.deploy, userName).flatMap { newOperationTrace =>
      val allSuccessesAndFailures = executionSpecs.map { executionSpec =>
        // todo: retrieve the real target of the very retried execution
        val expandedTarget = expandTarget(resolver, deploymentRequest.product.name, deploymentRequest.version, deploymentRequest.parsedTarget)
        val invocations = dispatch(dispatcher, expandedTarget, executionSpec.specificParameters).toSeq
        startExecution(invocations, deploymentRequest, newOperationTrace, executionSpec)
      }
      Future.sequence(allSuccessesAndFailures).map(_.foldLeft((0, 0)) { case (initialValue, (successes, failures)) =>
        (initialValue._1 + successes, initialValue._2 + failures)
      }).map { case (started, failed) => (newOperationTrace, started, failed) }
    }
  }

  def startExecution(invocations: Seq[(ExecutorInvoker, TargetExpr)],
                     deploymentRequest: DeepDeploymentRequest,
                     operationTrace: OperationTrace,
                     executionSpecification: ExecutionSpecification): Future[(Int, Int)] = {
    val productName = deploymentRequest.product.name
    val version = executionSpecification.version

    dbBinding.insertExecution(operationTrace.id, executionSpecification.id).flatMap(executionId =>
      // create as many traces, all at the same time
      dbBinding.insertExecutionTraces(executionId, invocations.length)
        .map(x => invocations.zip(x))
        .flatMap { executionSpecs =>
          // and only then, for each execution to do:
          Future.traverse(executionSpecs) {
            case ((executor, target), execTraceId) =>
              // log the execution
              logger.debug(s"Triggering ${operationTrace.kind} job for execution #$execTraceId of $productName v. $version on $executor")
              // trigger the execution
              val trigger = try {
                executor.trigger(
                  execTraceId,
                  productName,
                  version,
                  target,
                  deploymentRequest.creator
                )
              } catch {
                case e: Throwable => Future.failed(new Exception("Could not trigger the execution; please contact #sre-perpetual", e))
              }
              trigger
                .flatMap(
                  // if that answers a log href, update the trace with it, and consider that the job
                  // is running (i.e. already followable and not yet terminated, really)
                  _.map(logHref =>
                    dbBinding.updateExecutionTrace(execTraceId, ExecutionState.running, "", logHref).map { updated =>
                      assert(updated)
                      (true, s"`$logHref` succeeded")
                    }
                  ).getOrElse(
                    Future.successful((true, "succeeded (but with an unknown log href)"))
                  )
                )
                .recoverWith {
                  // if triggering the job throws an error, mark the execution as failed at initialization
                  case e: Throwable =>
                    logger.error(e.getMessage, e)
                    dbBinding.updateExecutionTrace(execTraceId, ExecutionState.initFailed, e.getMessage).map { updated =>
                      assert(updated)
                      (false, s"failed (${e.getMessage})")
                    }
                }
                .map { case (succeeded, identifier) =>
                  logExecution(identifier, execTraceId, executor, target)
                  succeeded
                }
          }.map { statuses =>
            val successes = statuses.count(s => s)
            (successes, statuses.length - successes)
          }
        }
    )
  }

  def revert(dispatcher: TargetDispatcher, deploymentRequest: DeepDeploymentRequest, userName: String, defaultVersion: Option[Version]): Future[(OperationTrace, Int, Int)] = {
    dbBinding
      .findExecutionSpecificationsForRevert(deploymentRequest)
      .flatMap { case (undetermined, determined) =>
        if (undetermined.nonEmpty)
          defaultVersion.map { version =>
            val specificParameters = dispatcher.freezeParameters(deploymentRequest.product.name, version)
            dbBinding.insertExecutionSpecification(specificParameters, version).map(executionSpecification =>
              Stream.cons((executionSpecification, undetermined), determined.toStream)
            )
          }.getOrElse(throw UnprocessableAction(
            s"a default rollback version is required, as some targets have no known previous state (e.g. `${undetermined.head}`)",
            detail = Map("required" -> "defaultVersion")
          ))
        else
          Future.successful(determined)
      }
      .flatMap { groups =>

        val executionSpecificationsAndInvocations = groups.map { case (execSpec, targets) =>
          (execSpec, dispatch(dispatcher, Set(TargetTerm(select = targets)), execSpec.specificParameters).toSeq)
        }

        dbBinding
          .insertOperationTrace(deploymentRequest.id, Operation.revert, userName)
          .flatMap(newOp =>
            Future.traverse(executionSpecificationsAndInvocations) { case (execSpec, invocations) =>
              startExecution(invocations, deploymentRequest, newOp, execSpec)
            }
              .map(_.fold((0, 0)) { case ((a, b), (c, d)) => (a + c, b + d) })
              .map { case (successes, failures) => (newOp, successes, failures) }
          )
      }
  }

  def expandTarget(resolver: TargetResolver, productName: String, productVersion: Version, target: TargetExpr): TargetExpr = {
    val select = target.select
    val toAtoms = resolver.toAtoms(productName, productVersion, select)

    checkUnchangedTarget(select, toAtoms.keySet, "resolution")
    toAtoms.foreach { case (word, atoms) =>
      if (atoms.isEmpty)
        throw UnprocessableIntent(s"`$word` is not a valid target in that context")
      atoms.foreach(atom => assert(atom.length <= TargetAtom.maxSize, s"Target `$atom` is too long"))
    }

    target.map(term => TargetTerm(term.tactics, term.select.iterator.flatMap(toAtoms).toSet))
  }

  private[engine] def dispatch(dispatcher: TargetDispatcher, expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutorInvoker, TargetExpr)] =
    dispatchAlternatives(dispatcher, expandedTarget, frozenParameters).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.minBy(_.toJson.compactPrint.length))
    }

  private[engine] def dispatchAlternatives(dispatcher: TargetDispatcher, expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutorInvoker, Set[TargetExpr])] = {
    def groupOn2[A, B](it: Iterable[(A, B)]): Map[B, Set[A]] =
      it.groupBy(_._2).map { case (k, v) => (k, v.map(_._1).toSet) }

    val perSelectAtom = groupOn2(
      expandedTarget.toStream.flatMap { case TargetTerm(tactics, select) =>
        select.toStream.flatMap(selectAtom =>
          tactics.map(tactic => (tactic, selectAtom)))
      }
    )

    // infer only once for all unique targets the executors required for each target word
    dispatchToExecutors(dispatcher, perSelectAtom.keySet, frozenParameters).map { case (executor, select) =>
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

  private[engine] def dispatchToExecutors(targetDispatcher: TargetDispatcher, targetAtoms: Select, frozenParameters: String) = {
    val dispatched = targetDispatcher.dispatch(targetAtoms, frozenParameters)
      .toStream
      .filter { case (_, select) => select.nonEmpty }

    val flattened: Select = dispatched.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet
    checkUnchangedTarget(targetAtoms, flattened, "dispatching")

    dispatched
  }

  private[engine] def checkUnchangedTarget(before: Select, after: Select, reason: String): Unit = {
    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    assert(after.subsetOf(before),
      s"More targets after $reason than before: " + (after -- before).map(_.toString).mkString(", "))
    require(after.size == before.size,
      s"Some targets have been lost in $reason: " + (before -- after).map(_.toString).mkString(", "))
  }

  protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, target: TargetExpr): Unit = {
    logger.debug(s"Triggering job $identifier for execution #$execId: $executor <- ${target.toJson.compactPrint}")
  }
}
