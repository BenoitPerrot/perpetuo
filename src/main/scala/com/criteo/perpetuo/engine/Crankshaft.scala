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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object DeploymentStatus extends Enumeration {
  val notStarted = Value("notStarted")
  val inProgress = Value("inProgress")
  val flopped = Value("flopped")
  val failed = Value("failed")
  val succeeded = Value("succeeded")
  val paused = Value("paused")

  def from(operationState: OperationEffectState.Value): DeploymentStatus.Value =
    operationState match {
      case OperationEffectState.inProgress => inProgress
      case OperationEffectState.flopped => flopped
      case OperationEffectState.failed => failed
      case OperationEffectState.succeeded => succeeded
    }
}

abstract class RejectingException extends RuntimeException {
  val msg: String
  val detail: Map[String, _]

  override def getMessage: String = msg + detail.map {
    case (k, v: Iterable[_]) => s"; $k: ${v.mkString(", ")}"
    case (k, v) => s"; $k: $v"
  }.mkString("")
}

case class UnprocessableIntent(msg: String, detail: Map[String, _] = Map()) extends RejectingException

case class UnavailableAction(msg: String, detail: Map[String, _] = Map()) extends RejectingException

case class MissingInfo(msg: String, required: String) extends RejectingException {
  val detail: Map[String, String] = Map("required" -> required)
}

case class Conflict(msg: String, deploymentRequestId: Long, conflicts: Iterable[_] = Seq()) extends RejectingException {
  val detail: Map[String, _] =
    Map("deploymentRequestId" -> deploymentRequestId) ++
      (if (conflicts.nonEmpty) Map("conflicts" -> conflicts) else Map())
}

case class Veto(msg: String, reason: String) extends RejectingException {
  val detail: Map[String, String] = Map("reason" -> reason)
}

@Singleton
class Crankshaft @Inject()(val dbBinding: DbBinding,
                           val targetResolver: TargetResolver, // TODO: move to Engine
                           val targetDispatcher: TargetDispatcher,
                           val listeners: Seq[AsyncListener],
                           val findTriggeredExecution: TriggeredExecutionFinder) extends Logging {

  import dbBinding.dbContext.profile.api._

  val fuelFilter = new FuelFilter(dbBinding)

  def assessingDeploymentState(deploymentRequest: DeploymentRequest): DBIOAction[DeploymentState, NoStream, Effect.Read] =
    dbBinding
      .isOutdated(deploymentRequest)
      .flatMap(isOutdated =>
        dbBinding
          .findingDeploymentRequestAndEffects(deploymentRequest.id)
          .map(_.get)
          .map((isOutdated, _))
      )
      .map { case (isOutdated, (_, deploymentPlanSteps, effects)) =>
        assert(deploymentPlanSteps.nonEmpty)
        val sortedEffects = effects.sortBy(-_.operationTrace.id)

        if (deploymentRequest.state.contains(DeploymentRequestState.abandoned))
          Abandoned(deploymentRequest, deploymentPlanSteps, sortedEffects)
        else if (isOutdated)
          Outdated(deploymentRequest, deploymentPlanSteps, sortedEffects)
        else {
          val idToDeploymentPlanStep = deploymentPlanSteps.map(planStep => planStep.id -> planStep).toMap

          sortedEffects.headOption
            .map { latestEffect =>
              latestEffect.operationTrace.kind match {
                case Operation.revert =>
                  latestEffect.state match {
                    case OperationEffectState.inProgress =>
                      RevertInProgress(deploymentRequest, deploymentPlanSteps, sortedEffects, latestEffect)

                    case OperationEffectState.succeeded =>
                      Reverted(deploymentRequest, deploymentPlanSteps, sortedEffects)

                    case OperationEffectState.failed | OperationEffectState.flopped =>
                      RevertFailed(deploymentRequest, deploymentPlanSteps, sortedEffects, latestEffect.deploymentPlanStepIds.flatMap(idToDeploymentPlanStep.get))
                  }

                case Operation.deploy =>
                  assert(latestEffect.deploymentPlanStepIds.size == 1)
                  val latestOperatedPlanStep = idToDeploymentPlanStep(latestEffect.deploymentPlanStepIds.head)
                  val operatedPlanSteps = sortedEffects.flatMap(_.deploymentPlanStepIds).distinct.flatMap(idToDeploymentPlanStep.get)
                  latestEffect.state match {
                    case OperationEffectState.inProgress =>
                      DeployInProgress(deploymentRequest, deploymentPlanSteps, sortedEffects, latestEffect)

                    case OperationEffectState.succeeded =>
                      fuelFilter
                        .findNextPlanStep(deploymentPlanSteps, latestOperatedPlanStep.id)
                        .map(
                          Paused(deploymentRequest, deploymentPlanSteps, sortedEffects, _, latestOperatedPlanStep, operatedPlanSteps)
                        )
                        .getOrElse(
                          Deployed(deploymentRequest, deploymentPlanSteps, sortedEffects, operatedPlanSteps)
                        )

                    case OperationEffectState.failed | OperationEffectState.flopped =>
                      sortedEffects
                        .find(_.targetStatuses.exists(_.code != Status.notDone))
                        .map(_ =>
                          DeployFailed(deploymentRequest, deploymentPlanSteps, sortedEffects, latestOperatedPlanStep, operatedPlanSteps)
                        )
                        .getOrElse(
                          DeployFlopped(deploymentRequest, deploymentPlanSteps, sortedEffects, latestOperatedPlanStep)
                        )
                  }
              }
            }
            .getOrElse(
              NotStarted(deploymentRequest, deploymentPlanSteps, sortedEffects, deploymentPlanSteps.head)
            )
        }
      }

  def assessDeploymentState(deploymentRequest: DeploymentRequest): Future[DeploymentState] =
    dbBinding.dbContext.db.run(assessingDeploymentState(deploymentRequest))

  def getProducts: Future[Seq[Product]] =
    dbBinding.getProducts

  def upsertProduct(productName: String, active: Boolean = true): Future[Product] =
    dbBinding.upsertProduct(productName, active)

  def setActiveProducts(names: Seq[String]): Future[Set[Product]] =
    dbBinding.setActiveProducts(names)

  def findDeploymentRequestById(deploymentRequestId: Long): Future[Option[DeploymentRequest]] =
    dbBinding.findDeploymentRequestById(deploymentRequestId)

  def createDeploymentRequest(protoDeploymentRequest: ProtoDeploymentRequest): Future[DeploymentRequest] =
    Future
      .sequence(listeners.map(_.onCreatingDeploymentRequest(protoDeploymentRequest)))
      .flatMap { _ =>
        // todo: replace that by the creation of all the related records when the first deploy operation will be created simultaneously
        targetDispatcher.freezeParameters(protoDeploymentRequest.productName, protoDeploymentRequest.version)

        dbBinding
          .insertDeploymentRequest(protoDeploymentRequest)
          .map { deploymentPlan =>
            Future.sequence(listeners.map(_.onDeploymentRequestCreated(deploymentPlan)))
            deploymentPlan.deploymentRequest
          }
      }

  private def triggerExecutions(deploymentRequest: DeploymentRequest,
                                operationTrace: OperationTrace,
                                executionsToTrigger: ExecutionsToTrigger): Future[(OperationTrace, Int, Int)] =
    triggerExecutions(deploymentRequest, executionsToTrigger).flatMap(effects =>
      Future.traverse(effects) { case (status, execTraceId, executionUpdate) =>
        executionUpdate
          .map { case (state, detail, href) =>
            // will close the operation if there is no more ongoing execution
            updateExecutionTrace(execTraceId, state, detail, href)
              .map(_ => status)
              .recover { case e =>
                // only log update errors, which are not critical in that case
                logger.error(s"Could not update execution trace #$execTraceId${href.map(s => s" ($s)").getOrElse("")} as $state: $detail", e)
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
        dbBinding.gettingDeploymentStatus(trace)
          .flatMap { status =>
            val transactionFinalSuccess = if (status == DeploymentStatus.paused)
              None
            else if (operationTrace.kind == Operation.revert || status == DeploymentStatus.flopped)
              Some(false)
            else if (status == DeploymentStatus.succeeded)
              Some(true)
            else
              None

            dbBinding.closingTargetStatuses(trace.id)
              .andThen(fuelFilter.releasingLocks(operationTrace.deploymentRequest, transactionFinalSuccess.isEmpty))
              .map(_ =>
                if (updated) { // if it's a successful close, let's notify
                  // FIXME: when/if partial reverts are supported: a _failed_ partial revert would be "paused" too, the operationSucceeded should be "false"
                  val operationSucceeded = status == DeploymentStatus.succeeded || status == DeploymentStatus.paused
                  listeners.foreach { listener =>
                    if (operationSucceeded)
                      listener.onOperationSucceeded(operationTrace)
                    else
                      listener.onOperationFailed(operationTrace)

                    transactionFinalSuccess.foreach(transactionSucceeded =>
                      if (transactionSucceeded)
                        listener.onDeploymentTransactionComplete(operationTrace.deploymentRequest)
                      else
                        listener.onDeploymentTransactionCanceled(operationTrace.deploymentRequest)
                    )
                  }
                }
              )
          }
          .map(_ => trace)
      }

  def checkState(deploymentRequest: DeploymentRequest, currentState: Int, expectedState: Int): Unit =
    if (currentState != expectedState)
      throw Conflict("the state of the deployment has just changed; have another look before choosing an action to trigger", deploymentRequest.id)

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
            .getOrElse(DBIO.successful(()))
        )
        .andThen(getOperationSpecifics)
        .flatMap { case ((deploymentPlanSteps, (operation, specAndInvocations, atoms)), actionSpecifics) =>
          fuelFilter.acquiringDeploymentTransactionLock(deploymentRequest, atoms.map(_.items))
            .andThen(dbBinding.insertingEffect(deploymentRequest, deploymentPlanSteps, operation, initiatorName, specAndInvocations, atoms.nonEmpty))
            .map((_, actionSpecifics))
        }
    ).flatMap { case ((createdOperation, executionsToTrigger), actionSpecifics) =>
      triggerExecutions(deploymentRequest, createdOperation, executionsToTrigger).map((_, actionSpecifics))
    }

  def step(deploymentRequest: DeploymentRequest,
           operationCount: Option[Int],
           initiatorName: String,
           getSpecifics: DBIOrw[(OperationCreationParams, Option[DeploymentPlanStep], DeploymentPlanStep)],
           emitEvent: Boolean = true): Future[OperationTrace] =
    act(
      deploymentRequest,
      operationCount,
      initiatorName,
      getSpecifics.map { case (creationParams, lastDone, toDo) => ((Seq(toDo), creationParams), (lastDone, toDo)) }
    )
      .map { case ((operationTrace, started, failed), (lastDone, toDo)) =>
        if (emitEvent) {
          if (lastDone.contains(toDo))
            listeners.foreach(_.onDeploymentRequestRetried(toDo, started, failed))
          else if (lastDone.isEmpty)
            listeners.foreach(_.onDeploymentRequestStarted(deploymentRequest, started, failed))
          else
            listeners.foreach(_.onDeploymentRequestResumed(toDo, started, failed))
        }
        operationTrace
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
      throw new RuntimeException(s"This kind of execution cannot be stopped: ${triggeredExecution.href}")
    )
    stop()
      .map { incompleteState =>
        val ret = Future.failed(new RuntimeException(s"Could not stop the execution ${triggeredExecution.href} (current state: $incompleteState)"))
        if (incompleteState != executionTrace.state) // override anyway the detail if the state is outdated
          updateExecutionTrace(executionTrace.id, incompleteState, "", None).flatMap(_ => ret)
        else
          ret
      }
      .getOrElse(
        updateExecutionTrace(executionTrace.id, ExecutionState.aborted, s"stopped by $initiatorName", None)
          .map { _ =>
            logger.info(s"Execution successfully stopped: ${triggeredExecution.href}")
            true
          }
          .recover {
            case _: UnavailableAction =>
              logger.warn(s"Execution already terminated: ${triggeredExecution.href}")
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
  def tryStopOperation(effectInProgress: OperationEffect, initiatorName: String): Future[(Int, Seq[String])] = {
    val openExecutionTraces = effectInProgress.executionTraces.filter(t => t.state == ExecutionState.pending || t.state == ExecutionState.running).toSeq
    val stoppingOperation =
      if (openExecutionTraces.nonEmpty)
        Future.traverse(openExecutionTraces) { openExecutionTrace =>
          try {
            stopExecution(openExecutionTrace, initiatorName).map(Left(_)).recover { case e => Right(e.getMessage) }
          }
          catch {
            case e: Throwable =>
              logger.error(s"While trying to stop execution #${openExecutionTrace.id} (${openExecutionTrace.href})", e)
              Future.successful(Right(e.getMessage))
          }
        }
      else // todo: maybe a better way would be to possibly close the operation each time the user is querying the status of this operation
        dbBinding.dbContext.db.run(closingOperation(effectInProgress.operationTrace))
          .map(_ => Seq())
    stoppingOperation
      .map { results =>
        val nbStopped = results.count(_.left.getOrElse(false))
        val errors = results.collect { case Right(message) => message }
        listeners.foreach(_.onDeploymentRequestStopped(effectInProgress.operationTrace.deploymentRequest, nbStopped, errors.length))
        (nbStopped, errors)
      }
  }

  def revert(deploymentRequest: DeploymentRequest, operationCount: Option[Int], initiatorName: String,
             gettingRevertSpecifics: DBIOrw[((Iterable[DeploymentPlanStep], OperationCreationParams), Unit)]): Future[OperationTrace] =
    act(deploymentRequest, operationCount, initiatorName, gettingRevertSpecifics)
      .map { case ((operationTrace, started, failed), _) =>
        listeners.foreach(_.onDeploymentRequestReverted(deploymentRequest, started, failed))
        operationTrace
      }

  def deviseRevertPlan(deploymentRequest: DeploymentRequest): Future[(Set[TargetAtom], Iterable[(ExecutionSpecification, Set[TargetAtom])])] = {
    val devisingRevertPlan =
      assessingDeploymentState(deploymentRequest)
        .flatMap {
          case _: Outdated =>
            DBIO.failed(Conflict("a newer one has already been applied", deploymentRequest.id))

          case _: Abandoned =>
            DBIO.failed(UnavailableAction("the deployment request has been abandoned", Map("deploymentRequestId" -> deploymentRequest.id)))

          case _: Reverted =>
            DBIO.failed(UnavailableAction("the deployment transaction is closed", Map("deploymentRequestId" -> deploymentRequest.id)))

          case _: NotStarted | _: DeployFlopped =>
            DBIO.failed(UnavailableAction("Nothing to revert", Map("deploymentRequestId" -> deploymentRequest.id)))

          case _: RevertInProgress | _: DeployInProgress =>
            DBIO.failed(UnavailableAction("another operation is already running", Map("deploymentRequestId" -> deploymentRequest.id)))

          case _: RevertFailed | _: DeployFailed | _: Paused | _: Deployed =>
            dbBinding.findingExecutionSpecificationsForRevert(deploymentRequest)
        }
    dbBinding.dbContext.db.run(devisingRevertPlan)
  }

  def findExecutionTracesByDeploymentRequest(deploymentRequestId: Long): Future[Option[Seq[ShallowExecutionTrace]]] =
    dbBinding.findExecutionTracesByDeploymentRequest(deploymentRequestId).flatMap { traces =>
      if (traces.isEmpty) {
        // if there is a deployment request with that ID, return the empty list, otherwise a 404
        dbBinding.deploymentRequestExists(deploymentRequestId).map(if (_) Some(traces) else None)
      }
      else
        Future.successful(Some(traces))
    }

  def updateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, href: Option[String], statusMap: Map[TargetAtom, TargetAtomStatus] = Map()): Future[Long] =
    tryUpdateExecutionTrace(id, executionState, detail, href, statusMap)
      .map(_.getOrElse(throw new IllegalArgumentException(s"Trying to update an execution trace ($id) that doesn't exist")))

  def tryUpdateExecutionTrace(id: Long, executionState: ExecutionState, detail: String, href: Option[String], statusMap: Map[TargetAtom, TargetAtomStatus] = Map()): Future[Option[Long]] =
    dbBinding.dbContext.db.run(
      dbBinding.findingBranchFromExecutionTraceId(id).flatMap(_
        .map { executionTraceBranch =>
          val op = executionTraceBranch.operationTrace
          dbBinding.updatingExecutionTrace(id, executionState, detail, href)
            .andThen(dbBinding.updatingTargetStatuses(executionTraceBranch.executionId, statusMap))
            .andThen(dbBinding.hasOpenExecutionTracesForOperation(op.id))
            .flatMap(hasOpenExecutions =>
              if (hasOpenExecutions)
                DBIO.successful(op)
              else
                closingOperation(op)
            )
            .map { _ =>
              // Calls listener for target status update
              statusMap.foreach { case (atom, value) =>
                // todo: check if already the target is in the same state and don't emit an annotation in this case
                if (value.code != Status.notDone)
                  listeners.foreach(_.onTargetAtomStatusUpdate(op, atom.name, value))
              }
              Some(op.id)
            }
        }
        .getOrElse(DBIO.successful(None))
      )
    )

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentPlan, DeploymentStatus.Value, Option[Operation.Kind])]] =
    dbBinding.findDeploymentRequestsWithStatuses(where, limit, offset)

  // todo: inline
  def computeState(operationEffect: OperationEffect): (Operation.Kind, OperationEffectState.Value) =
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
  def computeDominantVersion(productName: String, referenceAtoms: Iterable[Iterable[TargetAtom]]): Future[Option[Version]] = {
    // the fold function to chain the asynchronous queries as long as we get no result
    def chainQueries(acc: Future[Map[TargetAtom, Version]], atomsGroup: Iterable[TargetAtom]) =
      acc.flatMap(lastVersions =>
        if (lastVersions.isEmpty) // run the query below until we get a non-empty result
          dbBinding.findCurrentVersionForEachKnownTarget(productName, atomsGroup)
        else
          Future.successful(lastVersions)
      )

    referenceAtoms
      .foldLeft(Future.successful(Map[TargetAtom, Version]()))(chainQueries)
      .map { knownVersions =>
        // get the most represented version in that pool
        knownVersions.headOption.map { _ =>
          val (version, _) = knownVersions.values.groupBy(identity).maxBy { case (_, list) => list.size }
          version
        }
      }
  }

  private def getDeploySpecifics(dispatcher: TargetDispatcher,
                                 planStep: DeploymentPlanStep,
                                 expandedTarget: Option[TargetAtomSet],
                                 executionSpecs: Seq[ExecutionSpecification]): OperationCreationParams = {

    val specAndInvocations = executionSpecs.map(spec =>
      (spec, dispatcher.dispatchExpression(expandedTarget.getOrElse(planStep.parsedTarget), spec.specificParameters).toVector)
    )
    (Operation.deploy, specAndInvocations, expandedTarget)
  }

  def getStepSpecifics(expandedTarget: Option[TargetAtomSet],
                       planStep: DeploymentPlanStep): DBIOrw[OperationCreationParams] = {
    // generation of specific parameters
    val specificParameters = targetDispatcher.freezeParameters(planStep.deploymentRequest.product.name, planStep.deploymentRequest.version)

    // Create the execution specification outside of any transaction: it's not an issue if the request
    // fails afterward and the specification remains unbound.
    // Moreover, this will likely be rewritten eventually for the specifications to be created alongside with the
    // `deploy` operations at the time the deployment request is created.
    dbBinding.insertingExecutionSpecification(specificParameters, planStep.deploymentRequest.version).map(executionSpec =>
      getDeploySpecifics(targetDispatcher, planStep, expandedTarget, Seq(executionSpec))
    )
  }

  def getRetrySpecifics(expandedTarget: Option[TargetAtomSet],
                        planStep: DeploymentPlanStep,
                        effects: Seq[OperationEffect]): DBIOrw[OperationCreationParams] = {
    val successfulTargetAtoms = effects.filter(effect => effect.deploymentPlanStepIds.contains(planStep.id)).flatMap(_.targetStatuses).filter(_.code == Status.success).map(_.targetAtom)
    // todo: map the right target to the right specification
    dbBinding.findingDeploySpecifications(planStep).map(executionSpecs =>
      getDeploySpecifics(
        targetDispatcher,
        planStep,
        expandedTarget.map(atoms => TargetAtomSet(atoms.items -- successfulTargetAtoms)),
        executionSpecs
      )
    )
  }

  def getRevertSpecifics(deploymentRequest: DeploymentRequest,
                         defaultVersion: Option[Version]): DBIOrw[(Iterable[DeploymentPlanStep], OperationCreationParams)] = {
    dbBinding
      .findingExecutionSpecificationsForRevert(deploymentRequest)
      .flatMap { case (undetermined, determined) =>
        if (undetermined.nonEmpty)
          defaultVersion.map { version =>
            val specificParameters = targetDispatcher.freezeParameters(deploymentRequest.product.name, version)
            // Create the execution specification outside of any transaction: it's not an issue if the request
            // fails afterward and the specification remains unbound.
            dbBinding.insertingExecutionSpecification(specificParameters, version).map(executionSpecification =>
              Stream.cons((executionSpecification, undetermined), determined.toStream)
            )
          }.getOrElse(throw MissingInfo(
            s"a default rollback version is required, as some targets have no known previous state (e.g. `${undetermined.head}`)",
            "defaultVersion"
          ))
        else
          DBIO.successful(determined)
      }
      .flatMap { groups =>
        val specAndInvocations = groups.map { case (spec, targets) =>
          (spec, targetDispatcher.dispatchExpression(TargetAtomSet(targets), spec.specificParameters).toVector)
        }
        val atoms = TargetAtomSet(groups.flatMap { case (_, targets) => targets }.toSet)
        dbBinding.findingOperatedPlanSteps(deploymentRequest).map(steps =>
          (steps, (Operation.revert, specAndInvocations, Some(atoms)))
        )
      }
  }

  private def triggerExecutions(deploymentRequest: DeploymentRequest,
                                toTrigger: ExecutionsToTrigger): Future[Iterable[(Boolean, Long, Option[(ExecutionState, String, Option[String])])]] = {
    val productName = deploymentRequest.product.name
    Future.traverse(toTrigger) { case (execTraceId, version, target, executor) =>
      // log the execution
      logger.debug(s"Triggering job for execution #$execTraceId of $productName v. $version on $executor")
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
        .map(optHref =>
          // if that answers a href, update the trace with it, and consider that the job
          // is running (i.e. already followable and not yet terminated, really)
          optHref.map(href =>
            (true, s"`$href` succeeded", Some((ExecutionState.running, "", optHref))) // todo: change to pending once DREDD-725 is implemented
          ).getOrElse(
            (true, "succeeded (but with an unknown href)", None)
          )
        )
        .recover {
          // if triggering the job throws an error, mark the execution as failed at initialization
          case e: Throwable =>
            logger.error(e.getMessage, e)
            (false, s"failed (${e.getMessage})", Some((ExecutionState.initFailed, e.getMessage, None)))
        }
        .map { case (succeeded, identifier, toUpdate) =>
          logger.debug(s"Triggering job $identifier for execution #$execTraceId: $executor <- $target")
          (succeeded, execTraceId, toUpdate)
        }
    }
  }

  def abandoning(deploymentRequest: DeploymentRequest): DBIOAction[Unit, NoStream, Effect.Read with Effect.Write with Effect.Transactional] =
    if (deploymentRequest.state.contains(DeploymentRequestState.abandoned))
      DBIO.successful(()) // The request is already abandoned, we silently quit the operation
    else
      fuelFilter.acquiringOperationLock(deploymentRequest)
        .andThen(
          assessingDeploymentState(deploymentRequest).flatMap {
            case _: NotStarted | _: DeployFlopped =>
              dbBinding.abandoningDeploymentRequest(deploymentRequest.id)
                .flatMap(_ => fuelFilter.releasingLocks(deploymentRequest, false)).map(_ => ())
            case _: Abandoned =>
              DBIO.successful(())
            case _ =>
              DBIO.failed(UnavailableAction("cannot abandon request in the current state"))
          }
        )
        .transactionally


  /**
    * Abandon deployment request if it's either in NotStarted or DeployFlopped state.
    * We acquire an operation lock to prevent any other operation to be launched while the deployment is being abandoned.
    */
  def abandon(deploymentRequest: DeploymentRequest): Future[Unit] =
    dbBinding.dbContext.db.run(abandoning(deploymentRequest))
}
