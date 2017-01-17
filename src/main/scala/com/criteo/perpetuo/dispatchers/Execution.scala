package com.criteo.perpetuo.dispatchers

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.executors.ExecutorInvoker
import com.twitter.inject.Logging
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


@Singleton
class Execution @Inject()(val executionTraces: ExecutionTraceBinding) extends Logging {

  import executionTraces.profile.api._
  import spray.json.DefaultJsonProtocol._


  def trigger(db: Database, invocations: Seq[(ExecutorInvoker, String)],
              operation: Operation, deploymentRequest: DeploymentRequest,
              tactics: Tactics): Future[Seq[String]] = {
    // first, log the operation intent in the DB
    executionTraces.addToDeploymentRequest(db, deploymentRequest.id.get, operation).flatMap(
      // create as many traces, all at the same time
      executionTraces.addToOperationTrace(db, _, invocations.length).map {
        execIds =>
          assert(execIds.length == invocations.length)
          invocations.zip(execIds)
      }
    ).flatMap(
      // and only then, for each execution to do:
      Future.traverse(_) {
        case ((executor, rawTarget), execId) =>
          // log the execution
          logger.debug(s"Triggering $operation job for execution #$execId of ${deploymentRequest.productName} v. ${deploymentRequest.version})")
          // trigger the execution
          executor.trigger(
            operation, execId,
            deploymentRequest.productName, deploymentRequest.version,
            rawTarget,
            deploymentRequest.creator
          ).map(
            // if that answers a UUID, update the trace with it
            _.flatMap(uuid => executionTraces.updateExecutionTrace(db, execId)(uuid).map(_ => s"`$uuid`"))
          ).getOrElse(
            Future.successful("with unknown ID")
          ).map(
            (identifier: String) => {
              // log and return the success message
              val msg = s"Triggered job $identifier for execution #$execId"
              logger.debug(msg)
              msg
            }
          )
      }
    )
  }

  def dispatch(dispatcher: TargetDispatching, target: TargetExpr): Iterable[(ExecutorInvoker, String)] = {
    def groupOn1[A, B](it: Iterable[(A, B)]): Iterable[(A, Iterable[B])] =
      it.groupBy(_._1).map { case (k, v) => (k, v.map(_._2)) }

    def groupOn2[A, B](it: Iterable[(A, B)]): Iterable[(B, Iterable[A])] =
      it.groupBy(_._2).map { case (k, v) => (k, v.map(_._1)) }

    val targetExpanded = for {
      TargetTerm(tactics, select) <- target
      selectWord <- select
    } yield (tactics, selectWord)

    val allExpanded = groupOn2(targetExpanded.toSet) // make couples unique, then group by "select word"
      // just to infer the executors to call for each "select word", only once per unique word
      .flatMap { case (selectWord, groupedTactics) =>
      for {
        executor <- dispatcher.assign(selectWord)
        tactics <- groupedTactics
        tactic <- tactics
      } yield (executor, (selectWord, tactic))
    }

    // then group by executor
    groupOn1(allExpanded).map { case (executor, execGroup) =>
      // and by "the rest":
      val alternatives: Seq[TargetExpr] = Seq(
        groupOn2(groupOn1(execGroup)).map(TargetTerm.tupled), // either first by "select word" then grouping these words by common "tactics"
        groupOn2(groupOn2(execGroup)).map(_.swap).map(TargetTerm.tupled) // or first by tactic then grouping the tactics by common "select"
      )
      // create the JSON rendering for both alternatives
      val Seq(expr1, expr2) = alternatives.map(_.toJson.compactPrint)
      // finally return the shortest target expression for the executor
      (executor, if (expr1.length < expr2.length) expr1 else expr2)
    }
  }
}
