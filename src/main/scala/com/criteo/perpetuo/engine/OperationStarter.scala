package com.criteo.perpetuo.engine

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.engine.{Select, TargetExpr, TargetTerm}
import com.criteo.perpetuo.engine.invokers.ExecutorInvoker
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


case class UnprocessableAction(msg: String, detail: Map[String, _] = Map())
  extends RuntimeException(msg + detail.map { case (k, v) => s"; $k: $v" }.mkString(""))


class OperationStarter(val dbBinding: DbBinding) extends Logging {

  /**
    * Start all relevant executions and return the numbers of successful
    * and failed execution starts.
    */
  def start(resolver: TargetResolver, dispatcher: TargetDispatcher, deploymentRequest: DeepDeploymentRequest, operation: Operation.Kind, userName: String): Future[(OperationTrace, Int, Int)] = {
    // generation of specific parameters
    val executionKind = Operation.executionKind(operation)
    val specificParameters = dispatcher.freezeParameters(executionKind, deploymentRequest.product.name, deploymentRequest.version)
    // target resolution
    val expandedTarget = expandTarget(resolver, deploymentRequest.product.name, deploymentRequest.version, deploymentRequest.parsedTarget)

    dbBinding.insertOperationTrace(deploymentRequest.id, operation, userName).flatMap(operationTrace =>
      dbBinding.insertExecutionSpecification(specificParameters, deploymentRequest.version).flatMap(executionSpecification =>
        startExecution(dispatcher, deploymentRequest, operationTrace, executionSpecification, expandedTarget).map {
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
        startExecution(dispatcher, deploymentRequest, newOperationTrace, executionSpec, expandedTarget)
      }
      Future.sequence(allSuccessesAndFailures).map(_.foldLeft((0, 0)) { case (initialValue, (successes, failures)) =>
        (initialValue._1 + successes, initialValue._2 + failures)
      }).map { case (started, failed) => (newOperationTrace, started, failed) }
    }
  }

  def startExecution(dispatcher: TargetDispatcher,
                     deploymentRequest: DeepDeploymentRequest,
                     operationTrace: OperationTrace,
                     executionSpecification: ExecutionSpecification,
                     expandedTarget: TargetExpr): Future[(Int, Int)] = {
    val productName = deploymentRequest.product.name
    val version = executionSpecification.version

    // execution dispatching
    val invocations = dispatch(dispatcher, expandedTarget, executionSpecification.specificParameters).toSeq

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
                  Operation.executionKind(operationTrace.kind),
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

  def rollbackOperation(dispatcher: TargetDispatcher, deploymentRequest: DeepDeploymentRequest, userName: String, defaultVersion: Option[Version]): Future[(OperationTrace, Int, Int)] = {
    dbBinding
      .findExecutionSpecificationsForRollback(deploymentRequest)
      .flatMap { case (undetermined, determined) =>
        if (undetermined.nonEmpty)
          defaultVersion.map { version =>
            val specificParameters = dispatcher.freezeParameters(Operation.executionKind(Operation.revert), deploymentRequest.product.name, version)
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
        val opCreation = dbBinding.insertOperationTrace(deploymentRequest.id, Operation.revert, userName)
        Future.traverse(groups) { case (execSpec, targets) =>
          val expandedTarget = Set(TargetTerm(select = targets))
          opCreation.flatMap(newOp =>
            startExecution(dispatcher, deploymentRequest, newOp, execSpec, expandedTarget)
          )
        }
          .map(_.fold((0, 0)) { case ((a, b), (c, d)) => (a + c, b + d) })
          .flatMap { case (successes, failures) => opCreation.map(o => (o, successes, failures)) }
      }
  }

  private[engine] def expandTarget(resolver: TargetResolver, productName: String, productVersion: Version, target: TargetExpr): TargetExpr = {
    val toAtoms = (word: String) => {
      val atoms = resolver.toAtoms(productName, productVersion.toString, word)
      require(atoms.nonEmpty, s"Target word `$word` doesn't resolve to any atomic target")
      atoms.foreach(atom => require(atom.length <= TargetAtom.maxSize, s"Target `$atom` is too long"))
      atoms
    }
    target.map(term => TargetTerm(term.tactics, term.select.flatMap(toAtoms)))
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

    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    val dispatchedTargets: Select = dispatched.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet
    assert(dispatchedTargets.subsetOf(targetAtoms),
      "More targets after dispatching than before: " + (dispatchedTargets -- targetAtoms).map(_.toString).mkString(", "))
    require(dispatchedTargets.size == targetAtoms.size,
      "Some target atoms have no designated executors: " + (targetAtoms -- dispatchedTargets).map(_.toString).mkString(", "))

    dispatched
  }

  protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, target: TargetExpr): Unit = {
    logger.debug(s"Triggering job $identifier for execution #$execId: $executor <- ${target.toJson.compactPrint}")
  }
}
