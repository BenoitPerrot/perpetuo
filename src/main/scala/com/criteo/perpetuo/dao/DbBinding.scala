package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.{DeploymentStatus, Select}
import com.criteo.perpetuo.model._
import javax.inject.{Inject, Singleton}
import slick.jdbc.TransactionIsolation

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


@Singleton
class DbBinding @Inject()(val dbContext: DbContext)
  extends DbContextProvider
    with ProductBinder
    with DeploymentRequestBinder
    with DeploymentPlanStepBinder
    with StepOperationXRefBinder
    with OperationTraceBinder
    with ExecutionBinder
    with ExecutionSpecificationBinder
    with TargetStatusBinder
    with ExecutionTraceBinder
    with LockBinder
    with DeploymentRequestInserter {

  import dbContext.profile.api._

  def executeInSerializableTransaction[T](q: DBIOAction[T, NoStream, _]): Future[T] =
    dbContext.db.run(q.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))

  def findDeepDeploymentRequestAndEffects(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Iterable[OperationEffect])]] = {
    val q = deploymentRequestQuery
      .filter(_.id === deploymentRequestId)
      .join(productQuery).on { case (deploymentRequest, product) => deploymentRequest.productId === product.id }
      .joinLeft(operationTraceQuery).on { case ((deploymentRequest, _), operationTrace) => deploymentRequest.id === operationTrace.deploymentRequestId }
      .joinLeft(executionQuery).on { case ((_, operationTrace), execution) => operationTrace.map(_.id) === execution.operationTraceId }
      .joinLeft(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
      .map { case ((((deploymentRequest, product), operationTrace), execution), executionTrace) => (deploymentRequest, product, operationTrace, execution.map(_.id), executionTrace) }
      .result

      .flatMap(executionTracesTree =>
        executionTracesTree.headOption
          .map { case (deploymentRequest, product, _, _, _) =>
            val executionIdToOperationId = executionTracesTree.flatMap { case (_, _, operationTrace, executionId, _) => executionId.map(_ -> operationTrace.get.id.get) }.toMap

            // in a separate request to not create huge joins between two orthogonal tables: ExecutionTrace and TargetStatus
            targetStatusQuery
              .filter(_.executionId.inSet(executionIdToOperationId.keys))
              .result

              .map { targetStatuses =>
                val operationIdToTargetStatuses = targetStatuses.groupBy(targetStatus => executionIdToOperationId(targetStatus.executionId))

                val effects = executionTracesTree
                  .flatMap { case (_, _, operationTrace, _, executionTrace) => operationTrace.map((_, executionTrace)) }
                  .groupBy { case (operationTrace, _) => operationTrace.id.get }
                  .toStream
                  .map { case (_, operationGroup) =>
                    val (operationTrace, _) = operationGroup.head
                    val executionTraces = operationGroup.flatMap { case (_, executionTrace) => executionTrace.map(_.toExecutionTrace) }
                    val targetStatuses = operationIdToTargetStatuses.getOrElse(operationTrace.id.get, Seq()).map(_.toTargetStatus)
                    OperationEffect(operationTrace.toOperationTrace, executionTraces, targetStatuses)
                  }

                Some((deploymentRequest.toDeepDeploymentRequest(product), effects))
              }
          }
          .getOrElse(DBIO.successful(None))
      )

    dbContext.db.run(q.map(_.map { case (depReq, effects) => (depReq, effects.sortBy(_.operationTrace.id)) }))
  }

  def findingDeploySpecifications(deploymentRequest: DeepDeploymentRequest): DBIOAction[Seq[ExecutionSpecification], NoStream, Effect.Read] =
    operationTraceQuery
      .filter(op => op.deploymentRequestId === deploymentRequest.id && op.operation === Operation.deploy)
      .take(1)
      .join(executionQuery)
      .join(executionSpecificationQuery)
      .filter { case ((operationTrace, execution), executionSpec) =>
        execution.operationTraceId === operationTrace.id &&
          execution.executionSpecificationId === executionSpec.id
      }
      .map { case (_, executionSpec) => executionSpec }
      .result
      .map(_.map(_.toExecutionSpecification))

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeepDeploymentRequest, DeploymentStatus.Value, Option[Operation.Kind])]] = {
    val filtered = where.foldLeft(this.deploymentRequestQuery join this.productQuery on (_.productId === _.id)) { (queries, spec) =>
      val value = spec.getOrElse("equals", throw new IllegalArgumentException(s"Filters tests must be `equals`"))
      val fieldName = spec.getOrElse("field", throw new IllegalArgumentException(s"Filters must specify ̀`field`"))
      fieldName match {
        case "id" => try queries.filter(_._1.id === value.asInstanceOf[Number].longValue) catch {
          case _: NullPointerException | _: ClassCastException => throw new IllegalArgumentException("Filters on `id` must test against a number")
        }
        case "productName" => try queries.filter(_._2.name === value.asInstanceOf[String]) catch {
          case _: ClassCastException => throw new IllegalArgumentException("Filters on `productName` must test against a string value")
        }
        case _ => throw new IllegalArgumentException(s"Cannot filter on `$fieldName`")
      }
    }

    val q = filtered
      .sortBy { case (depReq, _) => depReq.id.desc }
      .drop(offset)
      .take(limit)

      .join(deploymentPlanStepQuery)
      .on { case ((deploymentRequest, _), planStep) => deploymentRequest.id === planStep.deploymentRequestId }

      // get all the execution traces branches, which have been inserted in a single transaction per operation
      .joinLeft(stepOperationXRefQuery)
      .on { case ((_, planStep), xref) => planStep.id === xref.deploymentPlanStepId }
      .joinLeft(operationTraceQuery)
      .on { case ((_, xref), operationTrace) => xref.map(_.operationTraceId) === operationTrace.id }
      .joinLeft(executionQuery)
      .on { case ((_, operationTrace), execution) => operationTrace.map(_.id) === execution.operationTraceId }
      .joinLeft(executionTraceQuery)
      .on { case ((_, execution), executionTrace) => execution.map(_.id) === executionTrace.executionId && executionTrace.state =!= ExecutionState.completed }
      .map { case ((((((deploymentRequest, product), planStep), _), operationTrace), _), executionTrace) =>
        (deploymentRequest, product, planStep.id, operationTrace.map((_, executionTrace.map(_.state))))
      }
      .result

      .flatMap { executionTraceBranches =>
        // in a separate request to not create huge joins between two orthogonal tables: ExecutionTrace and TargetStatus, and to only request the latter when necessary

        val deploymentStatuses = Seq.newBuilder[(DeepDeploymentRequest, DeploymentStatus.Value, Option[Operation.Kind])]

        val dependingOnTargetStatuses = executionTraceBranches
          .groupBy { case (deploymentRequest, _, _, _) => deploymentRequest.id.get }
          .values
          .map { group =>
            val (deploymentRequest, product, planStepId, lastEffect) = group.maxBy { case (_, _, _, effect) =>
              effect.map { case (op, executionState) => (op.id.get, executionState.isDefined) }.getOrElse((Long.MinValue, false))
            }
            val depReq = deploymentRequest.toDeepDeploymentRequest(product)
            lastEffect
              .map { case (operationTrace, executionTraceState) =>
                operationTrace.closingDate
                  .map { _ =>
                    val forward = operationTrace.operation == Operation.deploy
                    val notFinished = group.exists { case (_, _, stepId, _) => if (forward) planStepId < stepId else stepId < planStepId }
                    Some(operationTrace.id.get -> (depReq, operationTrace.operation, notFinished, executionTraceState))
                  }
                  .getOrElse {
                    deploymentStatuses.+=((depReq, DeploymentStatus.inProgress, Some(operationTrace.operation)))
                    None
                  }
              }
              .getOrElse {
                deploymentStatuses.+=((depReq, DeploymentStatus.notStarted, None))
                None
              }
          }
          .flatten
          .toMap

        if (dependingOnTargetStatuses.nonEmpty)
          executionQuery
            .join(targetStatusQuery)
            .on { case (execution, targetStatus) => execution.operationTraceId.inSet(dependingOnTargetStatuses.keySet) && execution.id === targetStatus.executionId }
            .map { case (execution, targetStatus) => (execution.operationTraceId, targetStatus.code) }
            .result
            .map { statuses =>
              val targetStatuses = statuses
                .groupBy { case (operationTraceId, _) => operationTraceId }
                .map { case (operationTraceId, group) => (operationTraceId, group.map { case (_, status) => status }) }
              dependingOnTargetStatuses.foreach { case (operationTraceId, (deploymentRequest, kind, notFinished, executionState)) =>
                val statuses = targetStatuses.getOrElse(operationTraceId, Seq())
                val state = computeOperationState(isRunning = false, executionState, statuses)
                val deploymentStatus = if (notFinished && (state == DeploymentStatus.succeeded || kind == Operation.revert)) DeploymentStatus.paused else state
                deploymentStatuses.+=((deploymentRequest, deploymentStatus, Some(kind)))
              }
              deploymentStatuses.result
            }
        else
          DBIO.successful(deploymentStatuses.result)
      }

    dbContext.db.run(q.map(_.sortBy { case (depReq, _, _) => -depReq.id }))
  }

  // todo: will likely be deprecated by the introduction of the deployment_plan_step and step_operation_xref tables
  def isDeploymentRequestStarted(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Boolean)]] = {
    dbContext.db.run(
      deploymentRequestQuery
        .filter(_.id === deploymentRequestId)
        .join(productQuery).on(_.productId === _.id)
        .joinLeft(operationTraceQuery).on { case ((depReq, _), operation) => depReq.id === operation.deploymentRequestId }
        .map { case (x, op) => (x, op.isDefined) }
        .result
    ).map(_.headOption.map { case ((depReq, product), started) => (depReq.toDeepDeploymentRequest(product), started) })
  }

  private def findingDeploymentPlanStepAndLatestOperations(deploymentRequestId: Long) =
    deploymentPlanStepQuery
      .filter(_.deploymentRequestId === deploymentRequestId)
      .joinLeft(stepOperationXRefQuery)
      .on { case (step, xref) => step.id === xref.deploymentPlanStepId }
      .joinLeft(operationTraceQuery)
      .on { case ((_, xref), operation) => xref.map(_.operationTraceId) === operation.id }
      .groupBy { case ((step, _), _) => step.id }
      .map { case (deploymentPlanStepId, q) =>
        (deploymentPlanStepId, q.map { case (_, operationTrace) => operationTrace.map(_.id) }.max)
      }
      .result

  private def getLastDoneStepAndOperation(planStepsAndLatestOperations: Seq[(Long, Option[Long])]) =
    planStepsAndLatestOperations.reduce[(Long, Option[Long])] {
      case ((latestPlanStepId, None), current@(planStepId, None)) if planStepId < latestPlanStepId =>
        // When comparing two steps, both with no operation, the oldest one is the latest one
        current

      case ((_, Some(latestOperationId)), current@(_, Some(operationId))) if latestOperationId < operationId =>
        // When comparing two steps, both with operations, the step with the youngest operation is the latest one
        current

      case ((_, None), current@(_, Some(_))) =>
        // When comparing a step with no operation to another with operations, the one with operations is the latest one
        current

      case (latest, _) =>
        // When all conditions failed, the latest one is to be kept
        latest
    }

  private def findNextPlanStepId(planStepIds: Seq[Long], referencePlanStepId: Long) =
    planStepIds
      .filter(referencePlanStepId < _)
      .reduceOption(_ min _)

  private def computingOperationStatus(operationId: Long, isRunning: Boolean): DBIOAction[DeploymentStatus.Value, NoStream, Effect.Read] =
    if (isRunning)
      DBIO.successful(DeploymentStatus.inProgress)
    else
      executionQuery
        .joinLeft(executionTraceQuery)
        .on { case (ex, et) => ex.operationTraceId === operationId && ex.id === et.executionId && et.state =!= ExecutionState.completed }
        .take(1)
        .joinLeft(targetStatusQuery)
        .on { case ((ex, _), ts) => ex.id === ts.executionId }
        .map { case ((_, et), ts) => (et.map(_.state), ts.map(_.code).getOrElse(Status.notDone)) }
        .distinctOn { case (_, targetState) => targetState }
        .result
        .map(statusSummary =>
          computeOperationState(
            isRunning,
            statusSummary.flatMap { case (execState, _) => execState },
            statusSummary.map { case (_, targetStatus) => targetStatus }
          )
        )

  private def gettingLastDoneAndToDoPlanStepId(planStepsAndLatestOperations: Seq[(Long, Option[Long])]): DBIOAction[(Option[Long], Option[Long]), NoStream, Effect.Read] =
    getLastDoneStepAndOperation(planStepsAndLatestOperations) match {
      case (latestPlanStepId, None) =>
        DBIO.successful((None, Some(latestPlanStepId)))

      case (latestPlanStepId, Some(operationId)) =>
        computingOperationStatus(operationId, isRunning = false)
          .map(operationStatus =>
            (Some(latestPlanStepId),
              if (operationStatus == DeploymentStatus.flopped || operationStatus == DeploymentStatus.failed)
                Some(latestPlanStepId)
              else
                findNextPlanStepId(planStepsAndLatestOperations.map(_._1), latestPlanStepId)
            )
          )
    }

  def gettingLastDoneAndToDoPlanStepId(deploymentRequestId: Long): DBIOAction[(Option[Long], Option[Long]), dbContext.profile.api.NoStream, Effect.Read with Effect.Read] =
    findingDeploymentPlanStepAndLatestOperations(deploymentRequestId)
      .flatMap(planStepsAndLatestOperations =>
        if (planStepsAndLatestOperations.isEmpty)
          DBIO.failed(new RuntimeException(s"$deploymentRequestId: should not be there: deployment plan is empty"))
        else
          gettingLastDoneAndToDoPlanStepId(planStepsAndLatestOperations)
      )

  // if that is removed one day (with multi-step, out-dating a deployment request makes less sense),
  // a few functions must be changed to not rely on current request application order, for instance
  // findExecutionSpecificationsForRevert
  def isOutdated(deploymentRequest: DeploymentRequest): Future[Boolean] = {
    val outdatedByOperation =
      deploymentRequestQuery
        .filter(depReq => depReq.id > deploymentRequest.id && depReq.productId === deploymentRequest.productId)
        .join(operationTraceQuery).on(_.id === _.deploymentRequestId)
        .exists
    dbContext.db.run(outdatedByOperation.result)
  }

  /**
    * @return the target atoms for which there is no previous execution specification on the same product,
    *         followed by the groups of target atoms sharing the same last execution specification for the same product.
    */
  def findExecutionSpecificationsForRevert(deploymentRequest: DeploymentRequest): Future[(Select, Iterable[(ExecutionSpecification, Select)])] = {
    val previousTargetStatuses = targetStatusQuery
      .join(executionQuery)
      .join(operationTraceQuery)
      .join(deploymentRequestQuery)
      .filter { case (((targetStatus, execution), operationTrace), oldDeploymentRequest) =>
        targetStatus.code =!= Status.notDone && targetStatus.executionId === execution.id &&
          execution.operationTraceId === operationTrace.id && operationTrace.deploymentRequestId === oldDeploymentRequest.id &&
          oldDeploymentRequest.productId === deploymentRequest.productId && oldDeploymentRequest.id < deploymentRequest.id
        // because it's impossible to apply deployment requests in another order than creation one
      }
      .map { case (((targetStatus, _), _), _) => targetStatus }

    val lastExecutionIdPerTarget = operationTraceQuery
      .join(executionQuery)
      .join(targetStatusQuery)
      .filter { case ((operationTrace, execution), targetStatus) =>
        targetStatus.code =!= Status.notDone && targetStatus.executionId === execution.id &&
          execution.operationTraceId === operationTrace.id && operationTrace.deploymentRequestId === deploymentRequest.id
      }
      .map { case (_, targetStatus) => targetStatus.targetAtom }
      .distinct
      .joinLeft(previousTargetStatuses)
      .on { case (impactedTargetAtom, anyTargetStatus) => impactedTargetAtom === anyTargetStatus.targetAtom }
      .groupBy { case (targetAtom, _) => targetAtom }
      .map { case (targetAtom, q) =>
        // todo: try to modify and reuse `latestExecutions` (it seems runtime-incompatible with the current query!)
        (targetAtom, q.map { case (_, targetStatus) => targetStatus.map(_.executionId) }.max)
      }

    val execSpecIds = lastExecutionIdPerTarget
      .joinLeft(
        executionQuery.join(executionSpecificationQuery).on(_.executionSpecificationId === _.id)
          .map { case (execution, execSpec) => (execution.id, execSpec) }
      )
      .on { case ((_, targetExecutionId), (anyExecutionId, _)) => targetExecutionId === anyExecutionId }
      .map { case ((targetAtom, _), specLink) => (targetAtom, specLink.map(_._2)) }

    dbContext.db.run(execSpecIds.result).map { perAtom =>
      type Targets = ArrayBuffer[TargetAtom.Type]
      val undetermined = new Targets
      var determined = Map[Long, (ExecutionSpecification, Targets)]()
      perAtom.foreach { case (targetAtom, specLink) =>
        specLink
          .map { spec =>
            determined
              .get(spec.id.get)
              .map { case (_, targets) => targets }
              .getOrElse {
                val targets = new Targets
                determined += spec.id.get -> (spec.toExecutionSpecification, targets)
                targets
              }
          }
          .getOrElse(
            undetermined
          ) += targetAtom
      }
      (undetermined.toSet, determined.values.map { case (execSpec, targets) => (execSpec, targets.toSet) })
    }
  }

  def findTargetAtomNotActionableBy(deploymentRequest: DeploymentRequest): Future[Option[TargetAtom.Type]] = {
    // The given deployment request has a specification for each target atom;
    // once we put aside the possible revert operations on this very deployment request (only),
    // for each target atom, if the last operation has the same specification as this deployment request,
    // then the deployment request is actionable: it can be retried, it can be reverted.
    // Note that if this deployment request has never been applied, it's also actionable.
    val query = operationTraceQuery
      .filter { op => op.deploymentRequestId === deploymentRequest.id && op.operation === Operation.deploy }
      .map(_.id)
      .sortBy(_.desc)
      .take(1)
      .join(
        targetStatusQuery
          .join(executionQuery)
          .join(operationTraceQuery)
          .join(deploymentRequestQuery)
          .filter { case (((targetStatus, execution), operationTrace), oldDeploymentRequest) =>
            targetStatus.executionId === execution.id && execution.operationTraceId === operationTrace.id &&
              operationTrace.deploymentRequestId === deploymentRequest.id && oldDeploymentRequest.productId === deploymentRequest.productId &&
              !(operationTrace.operation === Operation.revert && oldDeploymentRequest.id === deploymentRequest.id)
          }
          .groupBy { case (((targetStatus, _), _), _) => targetStatus.targetAtom }
          .map { case (targetAtom, q) => (targetAtom, q.map { case (((_, execution), _), _) => execution.id }.max) }
      )
      .join(executionQuery)
      .join(targetStatusQuery)
      .join(executionQuery)
      .filter { case ((((operationTraceId, (targetAtom, lastExecutionId)), testedExecution), targetStatus), execution) =>
        operationTraceId === testedExecution.operationTraceId && testedExecution.id === targetStatus.executionId &&
          targetStatus.targetAtom === targetAtom && lastExecutionId === execution.id
      }
      .map { case ((((_, (targetAtom, _)), testedExecution), _), lastExecution) =>
        (targetAtom, testedExecution.executionSpecificationId === lastExecution.executionSpecificationId)
      }

    dbContext.db.run(query.result)
      .map(_.collectFirst { case (targetAtom, actionable) if !actionable => targetAtom })
  }

  /** retrieve the "effect" (the leaves of the model tree) of an *existing* operation trace */
  def getOperationEffect(operationTrace: OperationTrace): Future[OperationEffect] = {
    val q = executionQuery
      .joinLeft(executionTraceQuery)
      .filter { case (execution, executionTrace) =>
        execution.operationTraceId === operationTrace.id && executionTrace.map(_.executionId) === execution.id
      }
      .map { case (execution, executionTrace) => (execution.id, executionTrace) }
      .result

      .flatMap { executionTraces =>
        // in a separate request to not create huge joins between two orthogonal tables: ExecutionTrace and TargetStatus:
        targetStatusQuery
          .filter(_.executionId.inSet(executionTraces.map { case (executionId, _) => executionId }))
          .result

          .map { targetStatuses =>
            val et = executionTraces.flatMap { case (_, executionTrace) => executionTrace.map(_.toExecutionTrace) }
            val ts = targetStatuses.map(_.toTargetStatus)
            OperationEffect(operationTrace, et, ts)
          }
      }

    dbContext.db.run(q)
  }

  private def latestExecutions(targetStatuses: Query[TargetStatusTable, TargetStatusRecord, Seq]) =
    targetStatuses
      .groupBy(_.targetAtom)
      .map { case (targetAtom, q) =>
        (targetAtom, q.map(_.executionId).max) // fixme: only true as long as the order of the requests is preserved
      }
      .join(executionQuery)
      .join(executionSpecificationQuery)
      .filter { case (((_, executionId), execution), execSpec) =>
        executionId === execution.id && execution.executionSpecificationId === execSpec.id
      }
      .map { case (((atom, _), _), execSpec) => (atom, execSpec) }

  /**
    * Compute the last version deployed on each given target if applicable for the given product,
    * with respect to the deployment history only.
    * Caution: it's hence a supposition about the current status of the targets as for the given product,
    * but unaware of the actual availability of the targets, for instance.
    *
    * @return for each target atom, the version assumed to be running (either successfully, failing, or still being deployed)
    */
  def findCurrentVersionForEachKnownTarget(productName: String, amongAtoms: Iterable[String]): Future[Map[String, Version]] = {
    val allStatuses = targetStatusQuery
      .join(executionQuery)
      .join(operationTraceQuery)
      .join(deploymentRequestQuery)
      .join(productQuery)
      .filter { case ((((ts, ex), op), dr), p) =>
        ts.targetAtom.inSet(amongAtoms) && ts.code =!= Status.notDone &&
          ts.executionId === ex.id && ex.operationTraceId === op.id &&
          op.deploymentRequestId === dr.id && dr.productId === p.id && p.name === productName
      }
      .map { case ((((ts, _), _), _), _) => ts }

    dbContext.db.run(
      latestExecutions(allStatuses)
        .map { case (atom, spec) => (atom, spec.version) }
        .result
    ).map(_.toMap)
  }
}
