package com.criteo.perpetuo.engine

import com.criteo.perpetuo.dao.{DBIOrw, DbBinding}
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.TriggeredExecutionFinder
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.google.common.annotations.VisibleForTesting
import com.twitter.inject.Logging
import javax.inject.{Inject, Singleton}
import slick.dbio.{DBIOAction, Effect, NoStream}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}


object DeploymentStatus extends Enumeration {
  val notStarted = Value("notStarted")
  val inProgress = Value("inProgress")
  val flopped = Value("flopped")
  val failed = Value("failed")
  val succeeded = Value("succeeded")
  val paused = Value("paused")
}

case class UnprocessableIntent(message: String) extends IllegalArgumentException(message)

abstract class RejectingError extends RuntimeException {
  val msg: String
  val detail: Map[String, _]

  override def getMessage: String = msg + detail.map {
    case (k, v: Iterable[_]) => s"; $k: ${v.mkString(", ")}"
    case (k, v) => s"; $k: $v"
  }.mkString("")
}

case class UnavailableAction(msg: String, detail: Map[String, _] = Map()) extends RejectingError

case class MissingInfo(msg: String, required: String) extends RejectingError {
  val detail: Map[String, String] = Map("required" -> required)
}

case class Conflict(msg: String, conflicts: Iterable[_] = Seq()) extends RejectingError {
  val detail: Map[String, Iterable[_]] = if (conflicts.nonEmpty) Map("conflicts" -> conflicts) else Map()
}

case class Veto(msg: String, reason: String) extends RejectingError {
  val detail: Map[String, String] = Map("reason" -> reason)
}

@Singleton
class Crankshaft @Inject()(val dbBinding: DbBinding,
                           val targetResolver: TargetResolver,
                           val targetDispatcher: TargetDispatcher,
                           val listeners: Seq[AsyncListener],
                           val findTriggeredExecution: TriggeredExecutionFinder) extends Logging {

  val fuelFilter = new FuelFilter(dbBinding)
  private val operationStarter = new OperationStarter(dbBinding)

  def getEligibleActions(deploymentRequest: DeploymentRequest): Future[Seq[(Operation.Kind, Option[String])]] =
    dbBinding.dbContext.db.run(
      fuelFilter.rejectingIfLocked(deploymentRequest)
        .andThen(
          DBIOAction.sequence(
            Operation.values.toSeq.map { action =>
              val canApply = action match {
                case Operation.deploy => fuelFilter.rejectingIfCannotDeploy(deploymentRequest)
                case Operation.revert => fuelFilter.rejectingIfCannotRevert(deploymentRequest)
              }
              canApply.asTry.collect {
                case Success(_) =>
                  (action, None)
                case Failure(e) =>
                  (action, Some(e.getMessage))
              }
            }
          )
        )
        // todo: add the stop action in the Seq:
        .asTry.map(_.getOrElse(Seq()))
    )

  def getProductNames: Future[Seq[String]] =
    dbBinding.getProductNames

  def insertProductIfNotExists(productName: String): Future[Product] =
    dbBinding.insertProductIfNotExists(productName)

  def findDeploymentRequestById(deploymentRequestId: Long): Future[Option[DeploymentRequest]] =
    dbBinding.findDeploymentRequestById(deploymentRequestId)

  def createDeploymentRequest(protoDeploymentRequest: ProtoDeploymentRequest): Future[DeploymentRequest] =
    Future
      .sequence(listeners.map(_.onCreatingDeploymentRequest(protoDeploymentRequest)))
      .flatMap { _ =>
        // todo: replace that by the creation of all the related records when the first deploy operation will be created simultaneously
        targetDispatcher.freezeParameters(protoDeploymentRequest.productName, protoDeploymentRequest.version)
        protoDeploymentRequest.plan.foreach(planStep =>
          operationStarter.expandTarget(targetResolver, protoDeploymentRequest.productName, protoDeploymentRequest.version, planStep.parsedTarget)
        )

        dbBinding
          .insertDeploymentRequest(protoDeploymentRequest)
          .map { deploymentPlan =>
            Future.sequence(listeners.map(_.onDeploymentRequestCreated(deploymentPlan.deploymentRequest)))
            deploymentPlan.deploymentRequest
          }
      }

  private def triggerExecutions(deploymentRequest: DeploymentRequest,
                                operationTrace: OperationTrace,
                                executionsToTrigger: ExecutionsToTrigger) =
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
        (operationTrace, successes.size, failures.size)
      }
    )

  private def closingOperation(operationTrace: OperationTrace): DBIOAction[OperationTrace, NoStream, Effect.Read with Effect.Write with Effect.Transactional] =
    dbBinding.closingOperationTrace(operationTrace)
      .map(_.map((_, true)).getOrElse((operationTrace, false)))
      .flatMap { case (trace, updated) =>
        dbBinding.gettingOperationEffect(trace)
          .flatMap { effect =>
            // todo: compute actual deployment status (with 'paused')
            val (kind, status) = computeState(effect)
            val transactionOngoing = kind == Operation.deploy && status == DeploymentStatus.failed
            dbBinding.closingTargetStatuses(trace.id)
              .andThen(fuelFilter.releasingLocks(operationTrace.deploymentRequest, transactionOngoing))
              .map(_ =>
                if (updated) { // if it's a successful close, let's notify
                  val handler = if (status == DeploymentStatus.succeeded)
                    (_: AsyncListener).onOperationSucceeded _
                  else
                    (_: AsyncListener).onOperationFailed _
                  listeners.foreach(listener => handler(listener)(trace))
                }
              )
          }
          .map(_ => trace)
      }

  private def checkState(deploymentRequest: DeploymentRequest, currentState: Int, expectedState: Int): Unit =
    if (currentState != expectedState)
      throw Conflict(s"${deploymentRequest.id}: the state of the deployment has just changed; have another look before choosing an action to trigger")

  private def act[T](deploymentRequest: DeploymentRequest, expectedOperationCount: Option[Int], initiatorName: String,
                     getOperationSpecifics: DBIOrw[((Iterable[DeploymentPlanStep], OperationCreationParams), T)]) =
    dbBinding.executeInSerializableTransaction(
      fuelFilter.acquiringOperationLock(deploymentRequest)
        .andThen(
          expectedOperationCount
            .map(expectedCount =>
              dbBinding.countingOperationTraces(deploymentRequest)
                .map(checkState(deploymentRequest, _, expectedCount))
            )
            .getOrElse(DBIOAction.successful(()))
        )
        .andThen(getOperationSpecifics)
        .flatMap { case ((deploymentPlanSteps, (operation, specAndInvocations, atoms)), actionSpecifics) =>
          fuelFilter.acquiringDeploymentTransactionLock(deploymentRequest, atoms)
            .andThen(dbBinding.insertingEffect(deploymentRequest, deploymentPlanSteps, operation, initiatorName, specAndInvocations, atoms.nonEmpty))
            .map((_, actionSpecifics))
        }
    ).flatMap { case ((createdOperation, executionsToTrigger), actionSpecifics) =>
      triggerExecutions(deploymentRequest, createdOperation, executionsToTrigger).map((_, actionSpecifics))
    }

  def step(deploymentRequest: DeploymentRequest, operationCount: Option[Int], initiatorName: String, emitEvent: Boolean = true): Future[OperationTrace] = {
    val getSpecifics = fuelFilter.gettingPlanStepToOperateAndLastDoneStep(deploymentRequest, Operation.deploy)
      .flatMap { case (toDo, lastDone) =>
        val isRetry = lastDone.contains(toDo)
        val getOperationSpecifics = if (isRetry) operationStarter.getRetrySpecifics _ else operationStarter.getStepSpecifics _
        getOperationSpecifics(targetResolver, targetDispatcher, toDo)
          .map(operationCreationSpecifics => ((Seq(toDo), operationCreationSpecifics), (lastDone, toDo)))
      }
    act(deploymentRequest, operationCount, initiatorName, getSpecifics)
      .map { case ((operationTrace, started, failed), (lastDone, toDo)) =>
        if (emitEvent)
          if (lastDone.contains(toDo)) {
            // TODO: rename onDeploymentRequestRetried to onDeploymentStepRetried(toDo)
            listeners.foreach(_.onDeploymentRequestRetried(deploymentRequest, started, failed))
          }
          else {
            if (lastDone.isEmpty) {
              listeners.foreach(_.onDeploymentRequestStarted(deploymentRequest, started, failed))
            }
            // TODO: fire onDeploymentStepStarted(toDo)
          }
        operationTrace
      }
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
  def tryStopDeploymentRequest(deploymentRequest: DeploymentRequest, operationCount: Option[Int], initiatorName: String): Future[(Int, Seq[String])] =
    dbBinding.dbContext.db
      .run(
        // validate the expected state and find the operation to close from the same request, for consistency yet without any lock
        dbBinding.findingOperationTracesByDeploymentRequest(deploymentRequest).flatMap { operationTraces =>
          operationCount.foreach(checkState(deploymentRequest, operationTraces.length, _))
          val operationTrace = operationTraces.maxBy(_.id)
          dbBinding.findingOpenExecutionTracesByOperationTrace(operationTrace.id)
            .map((operationTrace, _))
        }
      )
      .flatMap { case (operationTrace, openExecutionTraces) =>
        if (openExecutionTraces.nonEmpty)
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
        else // todo: maybe a better way would be to possibly close the operation each time the user is querying the status of this operation
          dbBinding.dbContext.db.run(closingOperation(operationTrace))
            .map(_ => Seq())
      }
      .map { results =>
        val nbStopped = results.count(_.left.getOrElse(false))
        val errors = results.collect { case Right(message) => message }
        listeners.foreach(_.onDeploymentRequestStopped(deploymentRequest, nbStopped, errors.length))
        (nbStopped, errors)
      }

  def revert(deploymentRequest: DeploymentRequest, operationCount: Option[Int], initiatorName: String, defaultVersion: Option[Version]): Future[OperationTrace] = {
    val getSpecifics = fuelFilter.rejectingIfCannotRevert(deploymentRequest)
      .andThen(operationStarter.getRevertSpecifics(targetDispatcher, deploymentRequest, initiatorName, defaultVersion))
      .map((_, ()))
    act(deploymentRequest, operationCount, initiatorName, getSpecifics)
      .map { case ((operationTrace, started, failed), _) =>
        listeners.foreach(_.onDeploymentRequestReverted(deploymentRequest, started, failed))
        operationTrace
      }
  }

  def deviseRevertPlan(deploymentRequest: DeploymentRequest): Future[(Select, Iterable[(ExecutionSpecification, Select)])] =
    dbBinding.dbContext.db.run(
      fuelFilter.rejectingIfLocked(deploymentRequest)
        .andThen(fuelFilter.rejectingIfCannotRevert(deploymentRequest))
        .andThen(dbBinding.findingExecutionSpecificationsForRevert(deploymentRequest))
    )

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowExecutionTrace]]] =
    dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def updateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, logHref: Option[String], statusMap: Map[String, TargetAtomStatus] = Map()): Future[Long] =
    tryUpdateExecutionTrace(id, executionState, detail, logHref, statusMap).map(_.getOrElse(throw new AssertionError(s"Trying to update an execution trace ($id) that doesn't exist")))

  def tryUpdateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, logHref: Option[String], statusMap: Map[String, TargetAtomStatus] = Map()): Future[Option[Long]] =
    dbBinding.dbContext.db.run(
      dbBinding.findingBranchFromExecutionTraceId(id).flatMap(_
        .map { executionTraceBranch =>
          val op = executionTraceBranch.operationTrace
          dbBinding.updatingExecutionTrace(id, executionState, detail, logHref)
            .andThen(dbBinding.updatingTargetStatuses(executionTraceBranch.executionId, statusMap))
            .andThen(dbBinding.hasOpenExecutionTracesForOperation(op.id))
            .flatMap(hasOpenExecutions =>
              if (hasOpenExecutions)
                DBIOAction.successful(op)
              else
                closingOperation(op)
            )
            .map { _ =>
              // Calls listener for target status update
              statusMap.foreach { case (target, value) =>
                // todo: check if already the target is in the same state and don't emit an annotation in this case
                if (value.code != Status.notDone)
                  listeners.foreach(_.onTargetAtomStatusUpdate(op, target, value))
              }
              Some(op.id)
            }
        }
        .getOrElse(DBIOAction.successful(None))
      )
    )

  def findDeploymentRequestAndEffects(deploymentRequestId: Long): Future[Option[(DeploymentRequest, Seq[DeploymentPlanStep], Iterable[OperationEffect])]] =
    dbBinding.findDeploymentRequestAndEffects(deploymentRequestId)

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentRequest, DeploymentStatus.Value, Option[Operation.Kind])]] =
    dbBinding.findDeploymentRequestsWithStatuses(where, limit, offset)

  // todo: inline
  def computeState(operationEffect: OperationEffect): (Operation.Kind, DeploymentStatus.Value) =
    (operationEffect.operationTrace.kind, operationEffect.state)

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
