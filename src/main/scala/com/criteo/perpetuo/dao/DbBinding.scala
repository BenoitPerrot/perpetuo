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

  def findDeploySpecifications(deploymentRequest: DeepDeploymentRequest): Future[Seq[ExecutionSpecification]] = {
    dbContext.db.run(operationTraceQuery
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
    ).map(_.map(_.toExecutionSpecification))
  }

  def deepQueryDeploymentRequests(where: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Seq[(DeepDeploymentRequest, DeploymentStatus.Value, Option[Operation.Kind])]] = {
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

      // get the last operation for each deployment request in the provided query
      .joinLeft(operationTraceQuery).on(_._1.id === _.deploymentRequestId)
      .groupBy { case ((request, _), _) => request.id }
      .map { case (requestId, operations) => (requestId, operations.map { case (_, operation) => operation.map(_.id) }.max) }

      // since we lost everything but the selected operation trace ID (because of Slick and SQL backend limitations), let's get everything back
      .join(productQuery)
      .join(deploymentRequestQuery)
      .filter { case (((requestId, _), product), request) => requestId === request.id && request.productId === product.id }
      .joinLeft(operationTraceQuery).on { case ((((_, operationId), _), _), operation) => operationId === operation.id }
      .map { case (((_, product), deploymentRequest), operationTrace) => ((deploymentRequest, product), operationTrace) }

      // get the execution traces
      .joinLeft(executionQuery).on { case ((_, operationTrace), execution) => operationTrace.map(_.id) === execution.operationTraceId }
      .joinLeft(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
      .map { case ((((deploymentRequest, product), operationTrace), execution), executionTrace) => (deploymentRequest, product, operationTrace, execution.map(_.id), executionTrace) }
      .result

      .flatMap { executionTracesTree =>
        val executionIdToOperationId = executionTracesTree.flatMap { case (_, _, operationTrace, executionId, _) => executionId.map(_ -> operationTrace.get.id.get) }.toMap

        // in a separate request to not create huge joins between two orthogonal tables: ExecutionTrace and TargetStatus
        targetStatusQuery
          .filter(_.executionId.inSet(executionIdToOperationId.keys))
          .result

          .map { targetStatuses =>
            val operationIdToTargetStatuses = targetStatuses.groupBy(targetStatus => executionIdToOperationId(targetStatus.executionId))

            executionTracesTree
              .groupBy { case (deploymentRequest, _, _, _, _) => deploymentRequest.id.get }
              .toStream
              .map { case (_, deploymentGroup) =>
                val (deploymentRequest, product, optOperationTrace, _, _) = deploymentGroup.head
                val effects = optOperationTrace.map { operationTrace =>
                  val executionTraces = deploymentGroup.flatMap { case (_, _, _, _, executionTrace) => executionTrace.map(_.toExecutionTrace) }
                  val targetStatuses = operationIdToTargetStatuses.getOrElse(operationTrace.id.get, Seq()).map(_.toTargetStatus)
                  OperationEffect(operationTrace.toOperationTrace, executionTraces, targetStatuses)
                }
                (deploymentRequest.toDeepDeploymentRequest(product), effects)
              }
          }
      }

    dbContext.db.run(q.map(_.sortBy { case (depReq, _) => -depReq.id }))
      .map(
        _.map { case (depReq, lastOperationEffect) =>
          (depReq, lastOperationEffect.map(effect => effect.state).getOrElse(DeploymentStatus.notStarted), lastOperationEffect.map(effect => effect.operationTrace.kind))
        }
      )
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
