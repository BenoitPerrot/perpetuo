package com.criteo.perpetuo.engine

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.TriggeredExecutionFinder
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.google.common.annotations.VisibleForTesting
import com.twitter.inject.Logging
import javax.inject.{Inject, Singleton}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try


object OperationStatus extends Enumeration {
  val notStarted = Value("notStarted")
  val inProgress = Value("inProgress")
  val flopped = Value("flopped")
  val failed = Value("failed")
  val succeeded = Value("succeeded")
}

case class UnprocessableIntent(message: String) extends IllegalArgumentException(message)

abstract class RejectingError extends RuntimeException {
  val msg: String
  val detail: Map[String, _]

  def copy(newMessage: String): RejectingError

  override def getMessage: String = msg + detail.map {
    case (k, v: Iterable[_]) => s"; $k: ${v.mkString(", ")}"
    case (k, v) => s"; $k: $v"
  }.mkString("")
}

case class UnavailableAction(msg: String, detail: Map[String, _] = Map()) extends RejectingError {
  def copy(newMessage: String) = UnavailableAction(newMessage, detail)
}

case class MissingInfo(msg: String, required: String) extends RejectingError {
  val detail: Map[String, String] = Map("required" -> required)

  def copy(newMessage: String) = MissingInfo(newMessage, required)
}

case class Conflict(msg: String, conflicts: Iterable[_] = Seq()) extends RejectingError {
  val detail: Map[String, Iterable[_]] = if (conflicts.nonEmpty) Map("conflicts" -> conflicts) else Map()

  def copy(newMessage: String) = Conflict(newMessage, conflicts)
}

case class Veto(msg: String, reason: String) extends RejectingError {
  val detail: Map[String, String] = Map("reason" -> reason)

  def copy(newMessage: String) = Veto(newMessage, reason)
}

@Singleton
class Crankshaft @Inject()(val dbBinding: DbBinding,
                           val targetResolver: TargetResolver,
                           val targetDispatcher: TargetDispatcher,
                           val listeners: Seq[AsyncListener],
                           val findTriggeredExecution: TriggeredExecutionFinder) extends Logging {

  // todo: cosmetics in attributes
  val config = AppConfigProvider.config

  private val withTransactions = !Try(config.getBoolean("noTransactions")).getOrElse(false)

  private val operationStarter = new OperationStarter(dbBinding)

  def getProductNames: Future[Seq[String]] =
    dbBinding.getProductNames

  def insertProductIfNotExists(productName: String): Future[Product] =
    dbBinding.insertProductIfNotExists(productName)

  def findDeepDeploymentRequestById(deploymentRequestId: Long): Future[Option[DeepDeploymentRequest]] =
    dbBinding.findDeepDeploymentRequestById(deploymentRequestId)

  def createDeploymentRequest(protoDeploymentRequest: ProtoDeploymentRequest): Future[DeepDeploymentRequest] =
    Future
      .sequence(listeners.map(_.onCreatingDeploymentRequest(protoDeploymentRequest)))
      .flatMap { _ =>
        // todo: replace that by the creation of all the related records when the first deploy operation will be created simultaneously
        targetDispatcher.freezeParameters(protoDeploymentRequest.productName, protoDeploymentRequest.version)
        operationStarter.expandTarget(targetResolver, protoDeploymentRequest.productName, protoDeploymentRequest.version, protoDeploymentRequest.parsedTarget)

        dbBinding
          .insertDeploymentRequest(protoDeploymentRequest)
          .map { deploymentRequest =>
            Future.sequence(listeners.map(_.onDeploymentRequestCreated(deploymentRequest)))
            deploymentRequest
          }
      }

  private def getOperationLockName(deploymentRequest: DeploymentRequest) =
    s"Operating on ${deploymentRequest.id}"

  private def getTransactionLockNames(deploymentRequest: DeepDeploymentRequest, atoms: Option[Set[String]]): Iterable[String] =
    atoms
      .map(_.map(atom => s"${atom.hashCode.toHexString}_${deploymentRequest.product.name}"))
      .getOrElse(Seq("P_" + deploymentRequest.product.name))

  private def releaseLocks(deploymentRequest: DeploymentRequest, transactionOngoing: Boolean) =
    if (transactionOngoing && withTransactions)
      dbBinding.releaseLock(getOperationLockName(deploymentRequest), deploymentRequest.id) // keep the locks per product/target
    else
      dbBinding.releaseLocks(deploymentRequest.id)

  private def startOperation(deploymentRequest: DeepDeploymentRequest,
                             operationStartSpecifics: OperationStartSpecifics): Future[(DeepOperationTrace, Int, Int)] =
    operationStartSpecifics
      .flatMap { case (recordsCreation, atoms) =>
        dbBinding.executeInSerializableTransaction(
          dbBinding.tryAcquireLocks(Seq(getOperationLockName(deploymentRequest)), deploymentRequest.id, reentrant = false).flatMap { alreadyRunning =>
            if (alreadyRunning.nonEmpty)
              throw Conflict(
                "Cannot be processed for the moment because another operation is running for the same deployment request"
              )

            dbBinding.tryAcquireLocks(getTransactionLockNames(deploymentRequest, atoms), deploymentRequest.id, reentrant = true).flatMap { conflictingRequestIds =>
              if (conflictingRequestIds.nonEmpty)
                throw Conflict(
                  "Cannot be processed for the moment because a conflicting transaction is ongoing, which must first succeed or be reverted",
                  conflictingRequestIds
                )

              recordsCreation
            }
          }
        )
      }
      .flatMap { case (createdOperation, executionsToTrigger) =>
        operationStarter.triggerExecutions(deploymentRequest, executionsToTrigger).flatMap(effects =>
          Future.traverse(effects) { case (status, execTraceId, executionUpdate) =>
            executionUpdate
              .map { case (state, detail, logHref) =>
                // will close the operation if there is no more ongoing execution
                updateExecutionTrace(execTraceId, state, detail, logHref)
                  .map(_ => status)
                  .recover { case e =>
                    // only log update errors, which are not critical in that case
                    logger.error(s"Could not update execution trace #$execTraceId${logHref.map(s => s" ($s)").getOrElse("")} as $state: $detail", e)
                    status
                  }
              }
              .getOrElse(Future.successful(status))
          }.map { statuses =>
            val (successes, failures) = statuses.partition(identity)
            (createdOperation, successes.size, failures.size)
          }
        )
      }

  // idempotent: on several attempts the listeners would be invoked at most once, but the DB updates
  // would be reapplied in an idempotent way (in the case something failed in the previous attempt)
  // because they are not done in a single transaction here
  private def closeOperation(operationTrace: ShallowOperationTrace, deploymentRequest: DeepDeploymentRequest): Future[ShallowOperationTrace] =
    dbBinding.closeOperationTrace(operationTrace)
      .map(_.map((_, true)).getOrElse((operationTrace, false)))
      .flatMap { case (trace, updated) =>
        dbBinding.getOperationEffect(trace)
          .flatMap { effect =>
            val (kind, status) = computeState(effect)
            val transactionOngoing = kind == Operation.deploy && status == OperationStatus.failed
            if (updated) {
              val handler = if (status == OperationStatus.succeeded)
                (_: AsyncListener).onOperationSucceeded _
              else
                (_: AsyncListener).onOperationFailed _
              listeners.foreach(listener => handler(listener)(trace, deploymentRequest))
            }
            Future.sequence(Seq(
              dbBinding.closeTargetStatuses(trace.id),
              releaseLocks(deploymentRequest, transactionOngoing)
            ))
          }
          .map(_ => trace)
      }

  def isDeploymentRequestStarted(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Boolean)]] =
    dbBinding.isDeploymentRequestStarted(deploymentRequestId)

  private def rejectIfOutdatedOrLocked(deploymentRequest: DeploymentRequest): Future[Unit] =
    Future
      .sequence(Seq(
        dbBinding.lockExists(getOperationLockName(deploymentRequest)).flatMap(
          if (_)
            Future.failed(Conflict(s"${deploymentRequest.id}: an operation is still running for it"))
          else
            Future.successful(())
        ),
        dbBinding.isOutdated(deploymentRequest).flatMap(
          if (_)
            Future.failed(Conflict(s"${deploymentRequest.id}: a newer one has already been applied"))
          else
            Future.successful(())
        )
      ))
      .map(_ => ())

  def canDeployDeploymentRequest(deploymentRequest: DeploymentRequest): Future[Unit] =
    rejectIfOutdatedOrLocked(deploymentRequest)

  def canRevertDeploymentRequest(deploymentRequest: DeploymentRequest, isStarted: Boolean): Future[Unit] =
    if (!isStarted)
      Future.failed(UnavailableAction("it has not yet been applied"))
    else {
      // todo: now we can allow successive rollbacks,
      // by using dbBinding.findTargetAtomNotActionableBy instead of `outdated` here
      rejectIfOutdatedOrLocked(deploymentRequest)
    }

  /**
    * Try to stop an execution from its trace
    *
    * @return true if it has been stopped, false if it was already terminated
    * @throws RuntimeException if it wasn't possible to stop the non-ended execution, for any reason
    */
  @VisibleForTesting
  def stopExecution(executionTrace: ShallowExecutionTrace, initiatorName: String): Future[Boolean] = {
    val triggeredExecution = findTriggeredExecution(executionTrace)
    val stop = triggeredExecution.stopper.getOrElse(
      throw new RuntimeException(s"This kind of execution cannot be stopped: ${triggeredExecution.logHref}")
    )
    stop()
      .map { incompleteState =>
        val ret = Future.failed(new RuntimeException(s"Could not stop the execution ${triggeredExecution.logHref} (current state: $incompleteState)"))
        if (incompleteState != executionTrace.state) // override anyway the detail if the state is outdated
          updateExecutionTrace(executionTrace.id, incompleteState, "", None).flatMap(_ => ret)
        else
          ret
      }
      .getOrElse(
        updateExecutionTrace(executionTrace.id, ExecutionState.stopped, s"by $initiatorName", None)
          .map { _ =>
            logger.info(s"Execution successfully stopped: ${triggeredExecution.logHref}")
            true
          }
          .recover {
            case _: UnavailableAction =>
              logger.warn(s"Execution already terminated: ${triggeredExecution.logHref}")
              false
          }
      )
  }

  /**
    * Try to stop all underlying executions in parallel: each one can failed for various predictable reasons.
    *
    * @return the number of successfully (newly) stopped executions and the list of failures (i.e. one error message
    *         for each execution that could not be stopped and is presumably still running).
    *         Executions that were already stopped are not included in the response.
    */
  def tryStopDeploymentRequest(deploymentRequest: DeepDeploymentRequest, initiatorName: String): Future[(Int, Seq[String])] =
    dbBinding
      .findOpenExecutionTracesByDeploymentRequest(deploymentRequest.id)
      .flatMap { openExecutionTraces =>
        Future.traverse(openExecutionTraces) { openExecutionTrace =>
          try {
            stopExecution(openExecutionTrace, initiatorName).map(Left(_)).recover { case e => Right(e.getMessage) }
          }
          catch {
            case e: Throwable =>
              logger.error(s"While trying to stop execution #${openExecutionTrace.id} (${openExecutionTrace.logHref})", e)
              Future.successful(Right(e.getMessage))
          }
        }
      }
      .map { results =>
        val nbStopped = results.collect { case Left(updated) if updated => 1 }.length
        val errors = results.collect { case Right(message) => message }
        listeners.foreach(_.onDeploymentRequestStopped(deploymentRequest, nbStopped, errors.length))
        (nbStopped, errors)
      }

  def startDeploymentStep(deploymentRequest: DeepDeploymentRequest, deploymentPlanStep: DeploymentPlanStep, initiatorName: String, emitEvent: Boolean = true): Future[DeepOperationTrace] =
    startOperation(deploymentRequest, operationStarter.startDeploymentStep(targetResolver, targetDispatcher, deploymentRequest, deploymentPlanStep, initiatorName))
      .map { case (operationTrace, started, failed) =>
        if (emitEvent)
          listeners.foreach(_.onDeploymentRequestStarted(deploymentRequest, started, failed))
        operationTrace
      }

  // TODO: remove: replace it by startDeploymentStep in callers
  def startDeploymentRequest(deploymentRequest: DeepDeploymentRequest, initiatorName: String, emitEvent: Boolean = true): Future[DeepOperationTrace] =
    dbBinding.findDeploymentPlan(deploymentRequest).flatMap { deploymentPlan =>
      assert(deploymentPlan.steps.size == 1)
      startDeploymentStep(deploymentRequest, deploymentPlan.steps.head, initiatorName, emitEvent)
    }

  def retryDeploymentStep(deploymentRequest: DeepDeploymentRequest, deploymentPlanStep: DeploymentPlanStep, initiatorName: String): Future[DeepOperationTrace] =
    dbBinding.findDeploySpecifications(deploymentRequest).flatMap(executionSpecs =>
      startOperation(deploymentRequest, operationStarter.retryDeploymentStep(targetResolver, targetDispatcher, deploymentRequest, deploymentPlanStep, executionSpecs, initiatorName))
        .map { case (operationTrace, started, failed) =>
          listeners.foreach(_.onDeploymentRequestRetried(deploymentRequest, started, failed))
          operationTrace
        }
    )

  // TODO: remove: replace it by retryDeploymentStep in callers
  def deployAgain(deploymentRequest: DeepDeploymentRequest, initiatorName: String): Future[DeepOperationTrace] =
    dbBinding.findDeploymentPlan(deploymentRequest).flatMap { deploymentPlan =>
      assert(deploymentPlan.steps.size == 1)
      retryDeploymentStep(deploymentRequest, deploymentPlan.steps.head, initiatorName)
    }

  def revert(deploymentRequest: DeepDeploymentRequest, initiatorName: String, defaultVersion: Option[Version]): Future[DeepOperationTrace] =
    startOperation(deploymentRequest, operationStarter.revert(targetDispatcher, deploymentRequest, initiatorName, defaultVersion))
      .map { case (operationTrace, started, failed) =>
        listeners.foreach(_.onDeploymentRequestReverted(deploymentRequest, started, failed))
        operationTrace
      }

  def findExecutionSpecificationsForRevert(deploymentRequest: DeploymentRequest): Future[(Select, Iterable[(ExecutionSpecification, Select)])] =
    dbBinding.findExecutionSpecificationsForRevert(deploymentRequest)

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowExecutionTrace]]] =
    dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  // idempotent
  def updateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, logHref: Option[String], statusMap: Map[String, TargetAtomStatus] = Map()): Future[Long] =
    tryUpdateExecutionTrace(id, executionState, detail, logHref, statusMap).map(_.getOrElse(throw new AssertionError(s"Trying to update an execution trace ($id) that doesn't exist")))

  // idempotent
  def tryUpdateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, logHref: Option[String], statusMap: Map[String, TargetAtomStatus] = Map()): Future[Option[Long]] =
    dbBinding.updateExecutionTrace(id, executionState, detail, logHref).flatMap(_
      .map(_ => // the execution trace exists
        dbBinding.findExecutionTraceById(id)
          .map(_.get)
          .flatMap { execTrace =>
            val op = execTrace.operationTrace
            dbBinding.dbContext.db.run(dbBinding.updateTargetStatuses(execTrace.executionId, statusMap))
              .flatMap(_ => dbBinding.hasOpenExecutionTracesForOperation(op.id))
              .flatMap(hasOpenExecutions =>
                if (hasOpenExecutions)
                  Future.successful(())
                else
                  dbBinding.findDeepDeploymentRequestById(op.deploymentRequestId).flatMap(depReq =>
                    closeOperation(op, depReq.get)
                  )
              )
          }
          .map(_ => Some(id))
      )
      .getOrElse(Future.successful(None))
    )

  def findDeepDeploymentRequestAndEffects(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Iterable[OperationEffect])]] =
    dbBinding.findDeepDeploymentRequestAndEffects(deploymentRequestId)

  def queryDeepDeploymentRequests(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[(DeepDeploymentRequest, Option[OperationEffect])]] =
    dbBinding.deepQueryDeploymentRequests(where, limit, offset)

  def computeState(operationEffect: OperationEffect): (Operation.Kind, OperationStatus.Value) = {
    val OperationEffect(operationTrace, executionTraces, targetStatuses) = operationEffect
    val lastOperationState = operationTrace.closingDate
      .map { _ =>
        if (targetStatuses.forall(_.code == Status.notDone))
          OperationStatus.flopped
        else if (targetStatuses.forall(_.code == Status.success) && executionTraces.forall(_.state == ExecutionState.completed))
          OperationStatus.succeeded
        else
          OperationStatus.failed
      }
      .getOrElse(OperationStatus.inProgress)
    (operationTrace.kind, lastOperationState)
  }

  /**
    * Compute the version that should be used in order to align the given target
    * with the main version deployed lately in reference targets, if applicable.
    *
    * @param referenceAtoms : ordered sequence of sets of target *atoms* whose "current" versions
    *                       must be inferred from the deployment history; the queries are applied
    *                       per set and in sequence until one responds at least a version.
    * @return the majority version of the first reference pool for which a version is found
    */
  def computeDominantVersion(productName: String, referenceAtoms: Iterable[Iterable[String]]): Future[Option[Version]] = {
    // the fold function to chain the asynchronous queries as long as we get no result
    def chainQueries(acc: Future[Map[String, Version]], atomsGroup: Iterable[String]) =
      acc.flatMap(lastVersions =>
        if (lastVersions.isEmpty) // run the query below until we get a non-empty result
          dbBinding.findCurrentVersionForEachKnownTarget(productName, atomsGroup)
        else
          Future.successful(lastVersions)
      )

    referenceAtoms
      .foldLeft(Future.successful(Map[String, Version]()))(chainQueries)
      .map { knownVersions =>
        // get the most represented version in that pool
        knownVersions.headOption.map { _ =>
          val (version, _) = knownVersions.values.groupBy(identity).maxBy { case (_, list) => list.size }
          version
        }
      }
  }

}
