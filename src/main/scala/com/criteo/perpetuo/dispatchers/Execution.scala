package com.criteo.perpetuo.dispatchers

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.dao.enums.Operation
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


  def startTransaction(db: Database, dispatcher: TargetDispatching, deploymentRequest: DeploymentRequest): (Future[Long], Future[Seq[String]]) = {
    require(deploymentRequest.id.isEmpty)

    // first, log the user's general intent
    val id = executionTraces.insert(db, deploymentRequest)

    // return futures on the ID and on the operation start messages
    (id, id.flatMap(depReqId => startOperation(db, dispatcher, deploymentRequest.copyWithId(depReqId), Operation.deploy)))
  }

  def startOperation(db: Database, dispatcher: TargetDispatching, deploymentRequest: DeploymentRequest, operation: Operation): Future[Seq[String]] = {
    require(deploymentRequest.id.isDefined)

    // infer dispatching
    val invocations = dispatch(dispatcher, deploymentRequest.parsedTarget).toSeq

    // log the operation intent in the DB
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
              logExecution(msg, executor, rawTarget)
              msg
            }
          )
      }
    )
  }

  def dispatch(dispatcher: TargetDispatching, target: TargetExpr): Iterable[(ExecutorInvoker, String)] =
    dispatchAlternatives(dispatcher, target).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.min(cmp = Ordering.by[String, Int](_.length)))
    }

  def dispatchAlternatives(dispatcher: TargetDispatching, target: TargetExpr): Iterable[(ExecutorInvoker, Set[String])] = {
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

  protected def logExecution(msg: String, executor: ExecutorInvoker, rawTarget: String): Unit = {
    logger.debug(s"$msg: $executor <- $rawTarget")
  }
}
