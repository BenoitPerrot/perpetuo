package com.criteo.perpetuo.engine

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.engine.dispatchers.{TargetDispatcher, TargetExpr, TargetTerm}
import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class OperationStarter(val dbBinding: DbBinding) extends Logging {

  /**
    * Start all relevant executions and return the numbers of successful
    * and failed execution starts.
    */
  def start(dispatcher: TargetDispatcher, deploymentRequest: DeploymentRequest, operation: Operation, userName: String): Future[(OperationTrace, Int, Int)] = {
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

  def retry(dispatcher: TargetDispatcher,
            deploymentRequest: DeploymentRequest,
            operationTrace: OperationTrace,
            executionSpecs: Seq[ExecutionSpecification],
            userName: String): Future[(OperationTrace, Int, Int)] = {

    dbBinding.insertOperationTrace(deploymentRequest.id, operationTrace.operation, userName).flatMap { newOperationTrace =>
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
                     deploymentRequest: DeploymentRequest,
                     operationTrace: OperationTrace,
                     executionSpecification: ExecutionSpecification,
                     expandedTarget: TargetExpr): Future[(Int, Int)] = {
    val productName = deploymentRequest.product.name
    val version = executionSpecification.version

    // execution dispatching
    val invocations = dispatch(dispatcher, expandedTarget).toSeq

    dbBinding.insertExecution(operationTrace.id, executionSpecification.id).flatMap(executionId =>
      // create as many traces, all at the same time
      dbBinding.insertExecutionTraces(operationTrace.id, executionId, invocations.length)
        .map(x => invocations.zip(x))
        .flatMap { executionSpecs =>
          // and only then, for each execution to do:
          Future.traverse(executionSpecs) {
            case ((executor, target), execTraceId) =>
              // log the execution
              logger.debug(s"Triggering ${operationTrace.operation} job for execution #$execTraceId of $productName v. $version on $executor")
              // trigger the execution
              executor
                .trigger(
                  execTraceId,
                  Operation.executionKind(operationTrace.operation),
                  productName,
                  version,
                  target,
                  executionSpecification.specificParameters,
                  deploymentRequest.creator
                )
                .flatMap(
                  // if that answers a log href, update the trace with it, and consider that the job
                  // is running (i.e. already followable and not yet terminated, really)
                  _.map(logHref =>
                    dbBinding.updateExecutionTrace(execTraceId, logHref, ExecutionState.running).map { updated =>
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
                    dbBinding.updateExecutionTrace(execTraceId, ExecutionState.initFailed).map { updated =>
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

  def rollbackOperation(dispatcher: TargetDispatcher, operationTrace: DeepOperationTrace, userName: String): Future[(OperationTrace, Int, Int)] = {
    dbBinding.findExecutionSpecIdsForRollback(operationTrace).flatMap { execSpecs =>
      val unknownPreviousState = execSpecs.find { case (_, execSpec) => execSpec.isEmpty }
      if (unknownPreviousState.isDefined) {
        val target: String = unknownPreviousState.get._1
        throw new IllegalArgumentException(s"Unknown previous state for target: $target")
      }
      else {
        val opCreation = dbBinding.insertOperationTrace(operationTrace.deploymentRequestId, Operation.revert, userName)
        Future.traverse(execSpecs.toStream.groupBy { case (_, execSpec) => execSpec.get.id }.values) { specifications =>
          val (_, execSpec) = specifications.head
          val targetAtoms = Set(TargetTerm(select = specifications.map(_._1).toSet))
          opCreation.flatMap(newOp =>
            startExecution(dispatcher, operationTrace.deploymentRequest, newOp, execSpec.get.toExecutionSpecification, targetAtoms)
          )
        }.map(_.fold((0, 0)) { case ((a, b), (c, d)) => (a + c, b + d) }).flatMap { case (successes, failures) => opCreation.map(o => (o, successes, failures)) }
      }
    }
  }

  private[engine] def dispatch(dispatcher: TargetDispatcher, target: TargetExpr): Iterable[(ExecutorInvoker, TargetExpr)] =
    dispatchAlternatives(dispatcher, target).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.minBy(_.toJson.compactPrint.length))
    }

  private[engine] def dispatchAlternatives(dispatcher: TargetDispatcher, target: TargetExpr): Iterable[(ExecutorInvoker, Set[TargetExpr])] = {
    def groupOn1[A, B](it: Iterable[(A, B)]): Iterable[(A, Set[B])] =
      it.groupBy(_._1).map { case (k, v) => (k, v.map(_._2).toSet) }

    def groupOn2[A, B](it: Iterable[(A, B)]): Iterable[(B, Set[A])] =
      it.groupBy(_._2).map { case (k, v) => (k, v.map(_._1).toSet) }

    val targetExpanded = for {
      TargetTerm(tactics, select) <- target
      selectWord <- select
    } yield (tactics, selectWord)

    val allExpanded = groupOn2(targetExpanded).flatMap { case (selectWord, groupedTactics) =>
      // just to infer only once per unique select word the executors to call
      for {
        executor <- dispatcher.dispatchToExecutors(selectWord)
        tactics <- groupedTactics
        tactic <- tactics
      } yield (executor, (selectWord, tactic))
    }

    // then group by executor
    groupOn1(allExpanded).map { case (executor, execGroup) =>
      // and by "the rest":
      val alternatives = Seq(
        groupOn2(groupOn1(execGroup)).map(TargetTerm.tupled), // either first by "select word" then grouping these words by common "tactics"
        groupOn2(groupOn2(execGroup)).map(_.swap).map(TargetTerm.tupled) // or first by tactic then grouping the tactics by common "select"
      )
      (executor, alternatives.map(_.toSet).toSet)
    }
  }

  protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, target: TargetExpr): Unit = {
    logger.debug(s"Triggering job $identifier for execution #$execId: $executor <- ${target.toJson.compactPrint}")
  }
}
