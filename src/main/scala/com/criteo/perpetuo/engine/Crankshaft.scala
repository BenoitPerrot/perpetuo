package com.criteo.perpetuo.engine

import com.criteo.perpetuo.auth.DeploymentAction
import com.criteo.perpetuo.dao.{DBIOrw, DbBinding}
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.{ExecutionTrigger, TriggeredExecutionFinder}
import com.criteo.perpetuo.engine.resolvers.TargetResolver
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model._
import com.google.common.annotations.VisibleForTesting
import com.twitter.inject.Logging
import javax.inject.{Inject, Singleton}
import slick.dbio.{DBIOAction, Effect, NoStream}
import spray.json.DefaultJsonProtocol._
import spray.json._

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
                           val targetResolver: TargetResolver,
                           val targetDispatcher: TargetDispatcher,
                           val listeners: Seq[AsyncListener],
                           val findTriggeredExecution: TriggeredExecutionFinder) extends Logging {

  val fuelFilter = new FuelFilter(dbBinding)

  def getEligibleActions(deploymentRequest: DeploymentRequest): Future[Seq[(DeploymentAction.Value, Operation.Kind, String, Option[String])]] =
    dbBinding.dbContext.db.run(
      fuelFilter.rejectingIfLocked(deploymentRequest)
        .andThen(
          DBIOAction.sequence(
            Operation.values.toSeq.map { operation =>
              val canApply = operation match {
                case Operation.deploy => fuelFilter.rejectingIfCannotDeploy(deploymentRequest)
                case Operation.revert => fuelFilter.rejectingIfCannotRevert(deploymentRequest)
              }
              val getRejectionReason = canApply.asTry.collect {
                case Success(_) => None
                case Failure(e: RejectingException) => Some(e.msg) // don't show the details
                case Failure(e) => Some(e.getMessage)
              }
              getRejectionReason.map((DeploymentAction.applyOperation, operation, operation.toString, _))
            }
          )
        )
        .asTry
        .flatMap(_
          .map(DBIOAction.successful)
          .getOrElse(
            findingLastOperationTrace(deploymentRequest.id, None).map(
              _.map(op => (DeploymentAction.stopOperation, op.kind, "stop", None)).toSeq
            )
          )
        )
    )

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
        protoDeploymentRequest.plan.foreach(planStep =>
          expandTarget(targetResolver, protoDeploymentRequest.productName, protoDeploymentRequest.version, planStep.parsedTarget)
        )

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

  private def checkState(deploymentRequest: DeploymentRequest, currentState: Int, expectedState: Int): Unit =
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
        val getOperationSpecifics = if (isRetry) getRetrySpecifics _ else getStepSpecifics _
        getOperationSpecifics(targetResolver, targetDispatcher, toDo)
          .map(operationCreationSpecifics => ((Seq(toDo), operationCreationSpecifics), (lastDone, toDo)))
      }
    act(deploymentRequest, operationCount, initiatorName, getSpecifics)
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
        updateExecutionTrace(executionTrace.id, ExecutionState.aborted, s"stopped by $initiatorName", None)
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

  def findLastOperationTrace(deploymentRequestId: Long, operationCount: Option[Int]): Future[Option[OperationTrace]] =
    dbBinding.dbContext.db.run(findingLastOperationTrace(deploymentRequestId, operationCount))

  // validate the expected state and find the operation to close from the same request, for consistency yet without any lock
  def findingLastOperationTrace(deploymentRequestId: Long, operationCount: Option[Int]): DBIOAction[Option[OperationTrace], NoStream, Effect.Read] =
    dbBinding.findingLastOperationTraceAndCurrentCountByDeploymentRequestId(deploymentRequestId).map(
      _.map { case (operationTrace, currentCount) =>
        operationCount.foreach(checkState(operationTrace.deploymentRequest, currentCount, _))
        operationTrace
      }
    )

  /**
    * Try to stop all underlying executions in parallel: each one can failed for various predictable reasons.
    *
    * @return the number of successfully (newly) stopped executions and the list of failures (i.e. one error message
    *         for each execution that could not be stopped and is presumably still running).
    *         Executions that were already stopped are not included in the response.
    */
  def tryStopOperation(operationTrace: OperationTrace, initiatorName: String): Future[(Int, Seq[String])] =
    dbBinding.dbContext.db.run(dbBinding.findingOpenExecutionTracesByOperationTrace(operationTrace.id))
      .flatMap(openExecutionTraces =>
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
      )
      .map { results =>
        val nbStopped = results.count(_.left.getOrElse(false))
        val errors = results.collect { case Right(message) => message }
        listeners.foreach(_.onDeploymentRequestStopped(operationTrace.deploymentRequest, nbStopped, errors.length))
        (nbStopped, errors)
      }

  def revert(deploymentRequest: DeploymentRequest, operationCount: Option[Int], initiatorName: String, defaultVersion: Option[Version]): Future[OperationTrace] = {
    val getSpecifics = fuelFilter.rejectingIfCannotRevert(deploymentRequest)
      .andThen(getRevertSpecifics(targetDispatcher, deploymentRequest, initiatorName, defaultVersion))
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
    tryUpdateExecutionTrace(id, executionState, detail, logHref, statusMap)
      .map(_.getOrElse(throw new IllegalArgumentException(s"Trying to update an execution trace ($id) that doesn't exist")))

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

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentPlan, DeploymentStatus.Value, Option[Operation.Kind])]] =
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

  private def getDeploySpecifics(dispatcher: TargetDispatcher,
                                 planStep: DeploymentPlanStep,
                                 expandedTarget: Option[TargetExpr],
                                 executionSpecs: Seq[ExecutionSpecification]): OperationCreationParams = {

    val specAndInvocations = executionSpecs.map(spec =>
      (spec, dispatch(dispatcher, expandedTarget.getOrElse(planStep.parsedTarget), spec.specificParameters).toVector)
    )
    (Operation.deploy, specAndInvocations, expandedTarget.map(_.flatMap(_.select)))
  }

  private[engine] def getStepSpecifics(resolver: TargetResolver,
                                       dispatcher: TargetDispatcher,
                                       planStep: DeploymentPlanStep): DBIOrw[OperationCreationParams] = {
    // generation of specific parameters
    val specificParameters = dispatcher.freezeParameters(planStep.deploymentRequest.product.name, planStep.deploymentRequest.version)
    // target resolution
    val expandedTarget = expandTarget(resolver, planStep.deploymentRequest.product.name, planStep.deploymentRequest.version, planStep.parsedTarget)

    // Create the execution specification outside of any transaction: it's not an issue if the request
    // fails afterward and the specification remains unbound.
    // Moreover, this will likely be rewritten eventually for the specifications to be created alongside with the
    // `deploy` operations at the time the deployment request is created.
    dbBinding.insertingExecutionSpecification(specificParameters, planStep.deploymentRequest.version).map(executionSpec =>
      getDeploySpecifics(dispatcher, planStep, expandedTarget, Seq(executionSpec))
    )
  }

  private def getRetrySpecifics(resolver: TargetResolver,
                                dispatcher: TargetDispatcher,
                                planStep: DeploymentPlanStep): DBIOrw[OperationCreationParams] = {
    // todo: map the right target to the right specification
    val expandedTarget = expandTarget(resolver, planStep.deploymentRequest.product.name, planStep.deploymentRequest.version, planStep.parsedTarget)

    dbBinding.findingDeploySpecifications(planStep).map(executionSpecs =>
      getDeploySpecifics(dispatcher, planStep, expandedTarget, executionSpecs)
    )
  }

  private def getRevertSpecifics(dispatcher: TargetDispatcher,
                                 deploymentRequest: DeploymentRequest,
                                 userName: String,
                                 defaultVersion: Option[Version]): DBIOrw[(Iterable[DeploymentPlanStep], OperationCreationParams)] = {
    dbBinding
      .findingExecutionSpecificationsForRevert(deploymentRequest)
      .flatMap { case (undetermined, determined) =>
        if (undetermined.nonEmpty)
          defaultVersion.map { version =>
            val specificParameters = dispatcher.freezeParameters(deploymentRequest.product.name, version)
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
          DBIOAction.successful(determined)
      }
      .flatMap { groups =>
        val specAndInvocations = groups.map { case (spec, targets) =>
          (spec, dispatch(dispatcher, Set(TargetTerm(select = targets)), spec.specificParameters).toVector)
        }
        val atoms = groups.flatMap { case (_, targets) => targets }
        dbBinding.findingOperatedPlanSteps(deploymentRequest).map(steps =>
          (steps, (Operation.revert, specAndInvocations, Some(atoms.toSet)))
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
        .map(optLogHref =>
          // if that answers a log href, update the trace with it, and consider that the job
          // is running (i.e. already followable and not yet terminated, really)
          optLogHref.map(logHref =>
            (true, s"`$logHref` succeeded", Some((ExecutionState.running, "", optLogHref))) // todo: change to pending once DREDD-725 is implemented
          ).getOrElse(
            (true, "succeeded (but with an unknown log href)", None)
          )
        )
        .recover {
          // if triggering the job throws an error, mark the execution as failed at initialization
          case e: Throwable =>
            logger.error(e.getMessage, e)
            (false, s"failed (${e.getMessage})", Some((ExecutionState.initFailed, e.getMessage, None)))
        }
        .map { case (succeeded, identifier, toUpdate) =>
          logger.debug(s"Triggering job $identifier for execution #$execTraceId: $executor <- ${target.toJson.compactPrint}")
          (succeeded, execTraceId, toUpdate)
        }
    }
  }

  private[engine] def expandTarget(resolver: TargetResolver, productName: String, productVersion: Version, target: TargetExpr): Option[TargetExpr] = {
    val select = target.select
    resolver.toAtoms(productName, productVersion, select).map { toAtoms =>
      checkUnchangedTarget(select, toAtoms.keySet, "resolution")
      toAtoms.foreach { case (word, atoms) =>
        if (atoms.isEmpty)
          throw UnprocessableIntent(s"`$word` is not a valid target in that context")
        atoms.foreach(atom => assert(atom.length <= TargetAtom.maxSize, s"Target `$atom` is too long"))
      }
      target.map(term => TargetTerm(term.tactics, term.select.iterator.flatMap(toAtoms).toSet))
    }
  }

  private[engine] def dispatch(dispatcher: TargetDispatcher, expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] =
    dispatchAlternatives(dispatcher, expandedTarget, frozenParameters).map {
      // return the shortest target expression for the executor
      case (executor, expressions) => (executor, expressions.minBy(_.toJson.compactPrint.length))
    }

  private[engine] def dispatchAlternatives(dispatcher: TargetDispatcher, expandedTarget: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, Set[TargetExpr])] = {
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

    val flattened: Select = dispatched.map { case (_, group) => group }.foldLeft(Stream.empty[String])(_ ++ _).toSet
    checkUnchangedTarget(targetAtoms, flattened, "dispatching")

    dispatched
  }

  private[engine] def checkUnchangedTarget(before: Select, after: Select, reason: String): Unit = {
    // check that we have the same targets before and after the dispatch (but one can be dispatched in several groups)
    assert(after.subsetOf(before),
      s"More targets after $reason than before: " + (after -- before).map(_.toString).mkString(", "))
    if (after.size != before.size)
      throw UnprocessableIntent("Unknown target(s): " + (before -- after).map(_.toString).mkString(", "))
  }
}
