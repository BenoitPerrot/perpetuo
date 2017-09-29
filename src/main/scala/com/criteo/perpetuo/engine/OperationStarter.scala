package com.criteo.perpetuo.engine

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.engine.dispatchers.{TargetDispatcher, TargetExpr, TargetTerm}
import com.criteo.perpetuo.engine.executors.ExecutorInvoker
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
  def start(dispatcher: TargetDispatcher, deploymentRequest: DeepDeploymentRequest, operation: Operation.Kind, userName: String): Future[(OperationTrace, Int, Int)] = {
    // generation of specific parameters
    val executionKind = Operation.executionKind(operation)
    val specificParameters = dispatcher.freezeParameters(executionKind, deploymentRequest.product.name, deploymentRequest.version)
    // target resolution
    val expandedTarget = dispatcher.expandTarget(deploymentRequest.product.name, deploymentRequest.version, deploymentRequest.parsedTarget)

    dbBinding.insertOperationTrace(deploymentRequest.id, operation, userName).flatMap(operationTrace =>
      dbBinding.insertExecutionSpecification(specificParameters, deploymentRequest.version).flatMap(executionSpecification =>
        startExecution(dispatcher, deploymentRequest, operationTrace, executionSpecification, expandedTarget).map {
          case (successes, failures) => (operationTrace, successes, failures)
        }
      )
    )
  }

  def deployAgain(dispatcher: TargetDispatcher,
                  deploymentRequest: DeepDeploymentRequest,
                  executionSpecs: Seq[ExecutionSpecification],
                  userName: String): Future[(OperationTrace, Int, Int)] = {

    dbBinding.insertOperationTrace(deploymentRequest.id, Operation.deploy, userName).flatMap { newOperationTrace =>
      val allSuccessesAndFailures = executionSpecs.map { executionSpec =>
        // todo: retrieve the real target of the very retried execution
        val expandedTarget = dispatcher.expandTarget(deploymentRequest.product.name, deploymentRequest.version, deploymentRequest.parsedTarget)
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
    val invocations = dispatch(dispatcher, expandedTarget).toSeq

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
                  executionSpecification.specificParameters,
                  deploymentRequest.creator
                )
              } catch {
                case e: Throwable => Future.failed(e)
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
          val targetAtoms = Set(TargetTerm(select = targets))
          opCreation.flatMap(newOp =>
            startExecution(dispatcher, deploymentRequest, newOp, execSpec, targetAtoms)
          )
        }
          .map(_.fold((0, 0)) { case ((a, b), (c, d)) => (a + c, b + d) })
          .flatMap { case (successes, failures) => opCreation.map(o => (o, successes, failures)) }
      }
  }

  private[engine] def dispatch(dispatcher: TargetDispatcher, expandedTarget: TargetExpr): Iterable[(ExecutorInvoker, TargetExpr)] =
    dispatchAlternatives(dispatcher, expandedTarget).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.minBy(_.toJson.compactPrint.length))
    }

  private[engine] def dispatchAlternatives(dispatcher: TargetDispatcher, expandedTarget: TargetExpr): Iterable[(ExecutorInvoker, Set[TargetExpr])] = {
    def groupOn2[A, B](it: Iterable[(A, B)]): Map[B, Set[A]] =
      it.groupBy(_._2).map { case (k, v) => (k, v.map(_._1).toSet) }

    val perSelectAtom = groupOn2(
      expandedTarget.toStream.flatMap { case TargetTerm(tactics, select) =>
        select.toStream.flatMap(selectAtom =>
          tactics.map(tactic => (tactic, selectAtom)))
      }
    )

    // infer only once for all unique targets the executors required for each target word
    dispatcher.dispatchToExecutors(perSelectAtom.keySet).map { case (executor, select) =>
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

  protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, target: TargetExpr): Unit = {
    logger.debug(s"Triggering job $identifier for execution #$execId: $executor <- ${target.toJson.compactPrint}")
  }
}
