package com.criteo.perpetuo.dispatchers

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.executors.ExecutorInvoker
import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model.{DeploymentRequest, ExecutionState}
import com.twitter.inject.Logging
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


@Singleton
class Execution @Inject()(val dbBinding: DbBinding) extends Logging {

  import spray.json.DefaultJsonProtocol._

  def startOperation(dispatcher: TargetDispatcher, deploymentRequest: DeploymentRequest, operation: Operation): Future[Int] = {
    // infer dispatching
    val invocations = dispatch(dispatcher, deploymentRequest.parsedTarget).toSeq

    // log the operation intent in the DB
    dbBinding.addToDeploymentRequest(deploymentRequest.id, operation).flatMap(
      // create as many traces, all at the same time
      dbBinding.addToOperationTrace(_, invocations.length).map {
        execIds =>
          assert(execIds.length == invocations.length)
          invocations.zip(execIds)
      }
    ).flatMap(
      // and only then, for each execution to do:
      Future.traverse(_) {
        case ((executor, rawTarget), execId) =>
          // log the execution
          logger.debug(s"Triggering $operation job for execution #$execId of ${deploymentRequest.product.id} v. ${deploymentRequest.version})")
          // trigger the execution
          executor.trigger(
            operation, execId,
            deploymentRequest.product.name, deploymentRequest.version,
            rawTarget,
            deploymentRequest.creator
          ).map(
            // if that answers a log href, update the trace with it, and consider that the job
            // is running (i.e. already followable and not yet terminated, really)
            _.map(logHref =>
              dbBinding.updateExecutionTrace(execId, logHref, ExecutionState.running).map { updated =>
                assert(updated)
                s"`$logHref` succeeded"
              }
            ).recover({
              // if triggering the job throws an error, mark the execution as failed at initialization
              case e: Throwable =>
                dbBinding.updateExecutionTrace(execId, ExecutionState.initFailed).map { updated =>
                  assert(updated)
                  s"failed (${e.getMessage})"
                }
            }).flatMap(x => x) // flatten doesn't exist on Future... :(
          ).getOrElse(
            Future.successful("succeeded (but with an unknown log href)")
          ).map(logExecution(_, execId, executor, rawTarget))
      }.map(_.length)
    )
  }

  def dispatch(dispatcher: TargetDispatcher, target: TargetExpr): Iterable[(ExecutorInvoker, String)] =
    dispatchAlternatives(dispatcher, target).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.minBy(_.length))
    }

  def dispatchAlternatives(dispatcher: TargetDispatcher, target: TargetExpr): Iterable[(ExecutorInvoker, Set[String])] = {
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
        executor <- dispatcher.assign(selectWord)
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
      // create the JSON rendering for both alternatives and return them both
      (executor, alternatives.map(_.toJson.compactPrint).toSet)
    }
  }

  protected def logExecution(identifier: String, execId: Long, executor: ExecutorInvoker, rawTarget: String): Unit = {
    logger.debug(s"Triggering job $identifier for execution #$execId: $executor <- $rawTarget")
  }
}
