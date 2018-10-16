package com.criteo.perpetuo.dao

import com.criteo.perpetuo.engine.{DeploymentStatus, computeDeploymentStatus}
import com.criteo.perpetuo.model._
import javax.inject.{Inject, Singleton}
import slick.jdbc.TransactionIsolation
import slick.sql.FixedSqlAction

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
    with DeploymentRequestInserter
    with EffectInserter {

  import dbContext.profile.api._

  def executeInSerializableTransaction[T](q: DBIOAction[T, NoStream, _]): Future[T] =
    dbContext.db.run(q.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))

  def findingDeploymentRequestAndEffects(deploymentRequestId: Long): DBIOAction[Option[(DeploymentRequest, Seq[DeploymentPlanStep], Stream[OperationEffect])], NoStream, Effect.Read] = {
    val findingDeploymentRequest =
      deploymentRequestQuery
        .join(productQuery)
        .join(deploymentPlanStepQuery)
        .filter { case ((deploymentRequest, product), planStep) =>
          deploymentRequest.id === deploymentRequestId && deploymentRequest.productId === product.id && planStep.deploymentRequestId === deploymentRequestId
        }
        .result

    val findingExecutionTraceTree =
      operationTraceQuery
        .filter(_.deploymentRequestId === deploymentRequestId)
        .joinLeft(executionQuery).on { case (operationTrace, execution) => operationTrace.id === execution.operationTraceId }
        .joinLeft(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
        .map { case ((operationTrace, execution), executionTrace) => (operationTrace, execution.map(_.id), executionTrace) }
        .result

    val findingXref =
      operationTraceQuery
        .join(stepOperationXRefQuery)
        .filter { case (operationTrace, xref) => operationTrace.deploymentRequestId === deploymentRequestId && operationTrace.id === xref.operationTraceId }
        .map { case (_, xref) => xref }
        .result

    val findingEffects =
      findingExecutionTraceTree
        .flatMap { executionTraceTree =>
          val executionIdToOperationId = executionTraceTree.flatMap { case (operationTrace, executionId, _) => executionId.map(_ -> operationTrace.id.get) }.toMap

          targetStatusQuery
            .filter(_.executionId.inSet(executionIdToOperationId.keys))
            .result

            .map { targetStatuses =>
              val operationIdToTargetStatuses = targetStatuses.groupBy(targetStatus => executionIdToOperationId(targetStatus.executionId))

              executionTraceTree
                .map { case (operationTrace, _, executionTrace) => (operationTrace, executionTrace) }
                .groupBy { case (operationTrace, _) => operationTrace.id.get }
                .toStream
                .map { case (_, operationGroup) =>
                  val (operationTrace, _) = operationGroup.head
                  val executionTraces = operationGroup.flatMap { case (_, executionTrace) => executionTrace.map(_.toExecutionTrace) }
                  val targetStatuses = operationIdToTargetStatuses.getOrElse(operationTrace.id.get, Seq()).map(_.toTargetStatus)
                  (operationTrace, executionTraces, targetStatuses)
                }
            }
        }

    findingDeploymentRequest.flatMap(deploymentIntent =>
      deploymentIntent
        .headOption
        .map { case ((deploymentRequestRecord, product), _) =>
          val deploymentRequest = deploymentRequestRecord.toDeploymentRequest(product)
          val deploymentPlanSteps = deploymentIntent.map { case (_, deploymentPlanStepRecord) => deploymentPlanStepRecord.toDeploymentPlanStep(deploymentRequest) }
          findingXref.flatMap { xrefs =>
            findingEffects.map { effects =>
              val operationTraceIdToPlanStepId = xrefs.groupBy(_.operationTraceId).mapValues(_.map(_.deploymentPlanStepId))
              Some((
                deploymentRequest,
                deploymentPlanSteps,
                effects.map { case (operationTrace, executionTraces, targetStatuses) =>
                  val op = operationTrace.toOperationTrace(deploymentRequest)
                  OperationEffect(op, operationTraceIdToPlanStepId(op.id), executionTraces, targetStatuses)
                }
              ))
            }
          }
        }
        .getOrElse(DBIO.successful(None))
    )
  }

  def findingDeploySpecifications(planStep: DeploymentPlanStep): DBIOAction[Seq[ExecutionSpecification], NoStream, Effect.Read] =
    operationTraceQuery
      .join(stepOperationXRefQuery)
      .filter { case (op, xref) =>
        op.deploymentRequestId === planStep.deploymentRequest.id && op.operation === Operation.deploy &&
          op.id === xref.operationTraceId && xref.deploymentPlanStepId === planStep.id
      }
      .take(1)
      .join(executionQuery)
      .join(executionSpecificationQuery)
      .filter { case (((operationTrace, _), execution), executionSpec) =>
        execution.operationTraceId === operationTrace.id &&
          execution.executionSpecificationId === executionSpec.id
      }
      .map { case (_, executionSpec) => executionSpec }
      .result
      .map(_.map(_.toExecutionSpecification))

  def findingOperatedPlanSteps(deploymentRequest: DeploymentRequest): DBIOAction[Seq[DeploymentPlanStep], NoStream, Effect.Read] =
    deploymentPlanStepQuery
      .join(stepOperationXRefQuery)
      .filter { case (planStep, xref) => planStep.deploymentRequestId === deploymentRequest.id && planStep.id === xref.deploymentPlanStepId }
      .groupBy { case (planStep, _) => planStep }
      .map { case (planStep, _) => planStep }
      .result
      .map(_.map(_.toDeploymentPlanStep(deploymentRequest)))

  private def queryDeploymentRequests(where: Seq[Map[String, Any]], limit: Int, offset: Int) = {
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

    filtered
      .sortBy { case (depReq, _) => depReq.id.desc }
      .drop(offset)
      .take(limit)
  }

  def findDeploymentRequests(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[DeploymentRequest]] =
    dbContext.db.run(
      queryDeploymentRequests(where, limit, offset).result
    ).map(
      _.map { case (deploymentRequest, product) =>
        deploymentRequest.toDeploymentRequest(product)
      }
    )

  def findDeploymentRequestsWithStatuses(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeploymentPlan, DeploymentStatus.Value, Option[Operation.Kind])]] = {
    val q = queryDeploymentRequests(where, limit, offset)
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
        (deploymentRequest, product, planStep, operationTrace.map((_, executionTrace.map(_.state))))
      }
      .result

      .flatMap { executionTraceBranches =>
        // in a separate request to not create huge joins between two orthogonal tables: ExecutionTrace and TargetStatus, and to only request the latter when necessary

        val deploymentStatuses = Seq.newBuilder[(DeploymentPlan, DeploymentStatus.Value, Option[Operation.Kind])]

        val dependingOnTargetStatuses = executionTraceBranches
          .groupBy { case (deploymentRequest, _, _, _) => deploymentRequest.id.get }
          .values
          .map { group =>
            val (deploymentRequest, product, planStep, lastEffect) = group.maxBy { case (_, _, _, effect) =>
              effect.map { case (op, executionState) => (op.id.get, executionState.isDefined) }.getOrElse((Long.MinValue, false))
            }
            val depReq = deploymentRequest.toDeploymentRequest(product)
            val planSteps = group.map { case (_, _, step, _) => step }.distinct.map(_.toDeploymentPlanStep(depReq)).sortBy(_.id)
            lastEffect
              .map { case (operationTrace, executionTraceState) =>
                operationTrace.closingDate
                  .map { _ =>
                    val forward = operationTrace.operation == Operation.deploy
                    val notFinished = group.exists { case (_, _, step, _) => if (forward) planStep.id.get < step.id.get else step.id.get < planStep.id.get }
                    Some(operationTrace.id.get -> (DeploymentPlan(depReq, planSteps), operationTrace.operation, notFinished, executionTraceState))
                  }
                  .getOrElse {
                    deploymentStatuses.+=((DeploymentPlan(depReq, planSteps), DeploymentStatus.inProgress, Some(operationTrace.operation)))
                    None
                  }
              }
              .getOrElse {
                deploymentStatuses.+=((DeploymentPlan(depReq, planSteps), DeploymentStatus.notStarted, None))
                None
              }
          }
          .flatten
          .toMap

        if (dependingOnTargetStatuses.nonEmpty)
          executionQuery
            .join(targetStatusQuery)
            .filter { case (execution, targetStatus) => execution.operationTraceId.inSet(dependingOnTargetStatuses.keySet) && execution.id === targetStatus.executionId }
            .map { case (execution, targetStatus) => (execution.operationTraceId, targetStatus.code) }
            .result
            .map { statuses =>
              val targetStatuses = statuses
                .groupBy { case (operationTraceId, _) => operationTraceId }
                .map { case (operationTraceId, group) => (operationTraceId, group.map { case (_, status) => status }) }
              dependingOnTargetStatuses.foreach { case (operationTraceId, (deploymentPlan, kind, notFinished, executionState)) =>
                val statuses = targetStatuses.getOrElse(operationTraceId, Seq())
                val state = computeOperationState(isRunning = false, executionState, statuses)
                val deploymentStatus = if (notFinished && (state == DeploymentStatus.succeeded || kind == Operation.revert)) DeploymentStatus.paused else state
                deploymentStatuses.+=((deploymentPlan, deploymentStatus, Some(kind)))
              }
              deploymentStatuses.result
            }
        else
          DBIO.successful(deploymentStatuses.result)
      }

    dbContext.db.run(q.map(_.sortBy { case (depPlan, _, _) => -depPlan.deploymentRequest.id }))
  }

  def findingLastOperationTraceAndCurrentCountByDeploymentRequestId(deploymentRequestId: Long): DBIOAction[Option[(OperationTrace, Int)], NoStream, Effect.Read] =
    operationTraceQuery
      .join(deploymentRequestQuery)
      .join(productQuery)
      .filter { case ((op, depReq), product) =>
        op.deploymentRequestId === deploymentRequestId && depReq.id === deploymentRequestId && depReq.productId === product.id
      }
      .result
      .map(results =>
        results
          .headOption
          .map { _ =>
            val ((operationTrace, depReq), product) = results.maxBy { case ((op, _), _) => op.id.get }
            (operationTrace.toOperationTrace(depReq.toDeploymentRequest(product)), results.length)
          }
      )

  def findingBranchFromExecutionTraceId(executionTraceId: Long): DBIOAction[Option[ExecutionTraceBranch], NoStream, Effect.Read] =
    executionTraceQuery
      .join(executionQuery)
      .join(operationTraceQuery)
      .join(deploymentRequestQuery)
      .join(productQuery)
      .filter { case ((((executionTrace, execution), operationTrace), deploymentRequest), product) =>
        executionTrace.id === executionTraceId &&
          executionTrace.executionId === execution.id && execution.operationTraceId === operationTrace.id &&
          operationTrace.deploymentRequestId === deploymentRequest.id && deploymentRequest.productId === product.id
      }
      .map { case ((((executionTrace, _), operationTrace), deploymentRequest), product) => (executionTrace, operationTrace, deploymentRequest, product) }
      .result
      .map(_.headOption.map { case (executionTrace, operationTrace, deploymentRequest, product) =>
        executionTrace.toExecutionTrace(operationTrace.toOperationTrace(deploymentRequest.toDeploymentRequest(product)))
      })

  def findingDeploymentPlanAndLatestOperations(deploymentRequest: DeploymentRequest): DBIOAction[Seq[(DeploymentPlanStep, Option[(DeploymentRequestId, Operation.Kind)])], NoStream, Effect.Read] =
    deploymentPlanStepQuery
      .filter(_.deploymentRequestId === deploymentRequest.id)
      .joinLeft(stepOperationXRefQuery)
      .on { case (step, xref) => step.id === xref.deploymentPlanStepId }
      .groupBy { case (step, _) => step.id }
      .map { case (deploymentPlanStepId, q) =>
        (deploymentPlanStepId, q.map { case (_, xref) => xref.map(_.operationTraceId) }.max)
      }
      .join(deploymentPlanStepQuery)
      .filter { case ((stepId, _), step) => stepId === step.id }
      .joinLeft(operationTraceQuery)
      .on { case (((_, lastOperationId), _), operationTrace) => lastOperationId === operationTrace.id }
      .map { case ((_, step), operationTrace) => (step, operationTrace.map(op => (op.id, op.operation))) }
      .result
      .map(_.map { case (step, lastOperation) =>
        (step.toDeploymentPlanStep(deploymentRequest), lastOperation)
      })

  // if that is removed one day (with multi-step, out-dating a deployment request makes less sense),
  // a few functions must be changed to not rely on current request application order, for instance
  // findExecutionSpecificationsForRevert
  def isOutdated(deploymentRequest: DeploymentRequest): FixedSqlAction[Boolean, NoStream, Effect.Read] =
    deploymentRequestQuery
      .join(operationTraceQuery)
      .filter { case (depReq, operationTrace) =>
        depReq.id > deploymentRequest.id && depReq.productId === deploymentRequest.productId && depReq.id === operationTrace.deploymentRequestId
      }
      .exists
      .result

  /**
    * @return the target atoms for which there is no previous execution specification on the same product,
    *         followed by the groups of target atoms sharing the same last execution specification for the same product.
    */
  def findingExecutionSpecificationsForRevert(deploymentRequest: DeploymentRequest): DBIOAction[(Set[TargetAtom], Iterable[(ExecutionSpecification, Set[TargetAtom])]), NoStream, Effect.Read] = {
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
        executionQuery
          .join(executionSpecificationQuery)
          .on(_.executionSpecificationId === _.id)
          .map { case (execution, execSpec) => (execution.id, execSpec) }
      )
      .on { case ((_, targetExecutionId), (anyExecutionId, _)) => targetExecutionId === anyExecutionId }
      .map { case ((targetAtom, _), specLink) => (targetAtom, specLink.map(_._2)) }

    execSpecIds.result.map { perAtom =>
      type Targets = ArrayBuffer[TargetAtom]
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
          ) += targetAtom.toModel
      }
      (undetermined.toSet, determined.values.map { case (execSpec, targets) => (execSpec, targets.toSet) })
    }
  }

  def findTargetAtomNotActionableBy(deploymentRequest: DeploymentRequest): Future[Option[TargetAtom]] = {
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
      .map(_.collectFirst { case (targetAtom, actionable) if !actionable => targetAtom.toModel })
  }

  def gettingOperationEffect(operationTrace: OperationTrace): DBIOAction[(Seq[Long], OperationEffect), NoStream, Effect.Read] =
    executionQuery
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
            (et, ts)
          }
      }
      .flatMap { case (et, ts) =>
        deploymentRequestQuery
          .join(deploymentPlanStepQuery)
          .filter { case (req, step) => req.id === operationTrace.deploymentRequest.id && req.id === step.deploymentRequestId }
          .joinLeft(stepOperationXRefQuery)
          .on { case ((_, step), xref) => xref.deploymentPlanStepId === step.id && xref.operationTraceId === operationTrace.id }
          .groupBy { case ((_, step), _) => step.id }
          .map { case (stepId, q) => (stepId, q.map { case (_, xref) => xref.map(_.operationTraceId) }.max.isDefined) }
          .result

          .map { result =>
            val planStepIds = result.map { case (stepId, _) => stepId }
            val impactedStepIds = result.collect { case (stepId, isImpacted) if isImpacted => stepId }
            (planStepIds, OperationEffect(operationTrace, impactedStepIds, et, ts))
          }
      }

  def gettingDeploymentStatus(operationTrace: OperationTrace): DBIOAction[DeploymentStatus.Value, NoStream, Effect.Read] =
    gettingOperationEffect(operationTrace).map { case (planStepIds, effect) =>
      computeDeploymentStatus(planStepIds, Some(effect))
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
  def findCurrentVersionForEachKnownTarget(productName: String, amongAtoms: Option[Iterable[TargetAtom]] = None): Future[Map[TargetAtom, Version]] = {
    val targetStatuses = targetStatusQuery
      .join(executionQuery)
      .join(operationTraceQuery)
      .join(deploymentRequestQuery)
      .join(productQuery)
      .filter { case ((((ts, ex), op), dr), p) =>
          ts.code =!= Status.notDone &&
          ts.executionId === ex.id && ex.operationTraceId === op.id &&
          op.deploymentRequestId === dr.id && dr.productId === p.id && p.name === productName
      }
      .map { case ((((ts, _), _), _), _) => ts }

    val allStatuses = amongAtoms
      .map(atoms => targetStatuses.filter(ts => ts.targetAtom.inSet(atoms.map(a => a: TargetAtomField))))
      .getOrElse(targetStatuses)

    dbContext.db.run(
      latestExecutions(allStatuses)
        .map { case (atom, spec) => (atom, spec.version) }
        .result
    ).map(_.map { case (k, v) => k.toModel -> v.toModel }.toMap)
  }

  def hasHadAnEffect(deploymentRequestId: Long): FixedSqlAction[Boolean, NoStream, Effect.Read] =
    targetStatusQuery
      .join(executionQuery)
      .join(operationTraceQuery)
      .filter { case ((ts, ex), op) =>
        ts.executionId === ex.id && ex.operationTraceId === op.id &&
          op.deploymentRequestId === deploymentRequestId && ts.code =!= Status.notDone
      }
      .exists
      .result

  def findExecutorName(executionTraceId: Long): Future[Option[String]] =
    dbContext.db.run(executionTraceQuery.filter(_.id === executionTraceId).map(_.executorType).result.headOption)
}
