package com.criteo.perpetuo.dispatchers

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.dao._
import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.executors.ExecutorInvoker
import com.twitter.inject.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


@Singleton
class Execution @Inject()(val executionTraces: ExecutionTraceBinding) extends Logging {

  import executionTraces.profile.api._


  def trigger(db: Database, invocations: Seq[(ExecutorInvoker, Select)],
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
        case ((executor, dispatchedSelect), execId) =>
          // log the execution
          logger.debug(s"Triggering $operation job for execution #$execId of ${deploymentRequest.productName} v. ${deploymentRequest.version})")
          // trigger the execution
          executor.trigger(
            operation, execId,
            deploymentRequest.productName, deploymentRequest.version,
            tactics, dispatchedSelect,
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
}
