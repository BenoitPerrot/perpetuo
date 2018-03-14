package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.engine.Select
import com.criteo.perpetuo.model._
import slick.jdbc.TransactionIsolation

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class Schema(val dbContext: DbContext)
  extends LockBinder with ExecutionTraceBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with TargetStatusBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  val all: dbContext.driver.DDL = productQuery.schema ++ deploymentRequestQuery.schema ++ operationTraceQuery.schema ++ executionSpecificationQuery.schema ++ executionQuery.schema ++ targetStatusQuery.schema ++ executionTraceQuery.schema ++ lockQuery.schema

  def createTables(): Unit = {
    Await.result(dbContext.db.run(all.create), 2.seconds)
  }
}


@Singleton
class DbBinding @Inject()(val dbContext: DbContext)
  extends DbContextProvider
    with DeploymentRequestBinder
    with ExecutionBinder
    with ExecutionSpecificationBinder
    with ExecutionTraceBinder
    with LockBinder
    with OperationTraceBinder
    with ProductBinder
    with TargetStatusBinder {

  import dbContext.driver.api._

  type DeepRecords = (DeploymentRequestRecord, ProductRecord, Option[OperationTraceRecord], Option[ExecutionTraceRecord], Option[TargetStatusRecord])

  def executeInSerializableTransaction[T](q: DBIOAction[T, NoStream, _]): Future[T] =
    dbContext.db.run(q.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))

  private def toOperationEffect(operationTrace: OperationTrace, joinedRecords: Seq[(Option[ExecutionTraceRecord], Option[TargetStatusRecord])]): OperationEffect = {
    val executionTraces = joinedRecords.flatMap(_._1).distinct.map(_.toExecutionTrace)
    val targetStatuses = joinedRecords.flatMap(_._2).distinct.map(_.toTargetStatus)
    OperationEffect(operationTrace, executionTraces, targetStatuses)
  }

  private def oneRequestRecordsToDeepModel(records: Iterable[DeepRecords]) = {
    // there must be at least one record, and all the records must relate to the same deployment request
    val deploymentRequest = records.head._1.toDeepDeploymentRequest(records.head._2)

    val sortedEffects = records
      .toStream
      .collect { case (_, _, operationTrace, executionTrace, targetStatus) if operationTrace.isDefined =>
        (operationTrace.get, executionTrace, targetStatus)
      }
      .groupBy { case (operationTrace, _, _) => operationTrace.id.get }
      .toStream
      .sortBy(_._1)
      .map { case (_, l) =>
        val (operationTraceRecord, _, _) = l.head
        val operationTrace = operationTraceRecord.toOperationTrace
        toOperationEffect(operationTrace, l.map { case (_, e, t) => (e, t) })
      }

    (deploymentRequest, sortedEffects)
  }

  def findDeepDeploymentRequestAndEffects(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Iterable[OperationEffect])]] = {
    dbContext.db.run(
      deploymentRequestQuery
        .filter(_.id === deploymentRequestId)
        .join(productQuery).on { case (deploymentRequest, product) => deploymentRequest.productId === product.id }
        .joinLeft(operationTraceQuery).on { case ((deploymentRequest, _), operationTrace) => deploymentRequest.id === operationTrace.deploymentRequestId }
        .joinLeft(executionQuery).on { case ((_, operationTrace), execution) => operationTrace.map(_.id) === execution.operationTraceId }
        .joinLeft(executionTraceQuery).on { case (((_, _), execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
        .joinLeft(targetStatusQuery).on { case ((((_, _), execution), _), targetStatus) => execution.map(_.id) === targetStatus.executionId }
        .map { case (((((deploymentRequest, product), operationTrace), _), executionTrace), targetStatus) => (deploymentRequest, product, operationTrace, executionTrace, targetStatus) }
        .result)

      .map(records =>
        records.headOption.map(_ => oneRequestRecordsToDeepModel(records))
      )
  }

  def findDeepDeploymentRequestAndSpecs(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Seq[ExecutionSpecification])]] = {
    dbContext.db.run(operationTraceQuery
      .filter(op => op.deploymentRequestId === deploymentRequestId && op.operation === Operation.deploy)
      .take(1)
      .join(executionQuery)
      .join(deploymentRequestQuery)
      .join(productQuery)
      .join(executionSpecificationQuery)
      .filter { case ((((operationTrace, execution), deploymentRequest), product), executionSpec) =>
        deploymentRequest.id === deploymentRequestId &&
          product.id === deploymentRequest.productId &&
          execution.operationTraceId === operationTrace.id &&
          execution.executionSpecificationId === executionSpec.id
      }
      .map { case (((_, deploymentRequest), product), executionSpec) => (deploymentRequest, product, executionSpec) }
      .result
    ).map(depReqAndExecSpecs =>
      depReqAndExecSpecs.headOption.map { case (deploymentRequest, product, _) =>
        (deploymentRequest.toDeepDeploymentRequest(product), depReqAndExecSpecs.map(_._3.toExecutionSpecification))
      }
    )
  }

  // TODO: find a cheap way to factor this - or find a more clever structure for the query
  // (note that as after type erasure the two methods have the same signature, they must have different names) <<
  private def sortBy(q: Query[(DeploymentRequestTable, ProductTable), (DeploymentRequestRecord, ProductRecord), scala.Seq], order: Seq[Map[String, Any]]) = {
    order.foldRight(q.sortBy(_._1.id)) { (spec, queries) =>
      val descending = try spec.getOrElse("desc", false).asInstanceOf[Boolean].value catch {
        case _: ClassCastException => throw new IllegalArgumentException("Orders `desc` must be true or false")
      }
      val fieldName = spec.getOrElse("field", throw new IllegalArgumentException(s"Orders must specify ̀`field`"))
      fieldName match {
        case "creationDate" => queries.sortBy(if (descending) _._1.creationDate.desc else _._1.creationDate.asc)
        case "creator" => queries.sortBy(if (descending) _._1.creator.desc else _._1.creator.asc)
        case "productName" => queries.sortBy(if (descending) _._2.name.desc else _._2.name.asc)
        case _ => throw new IllegalArgumentException(s"Cannot sort by `$fieldName`")
      }
    }
  }

  private def sortAllBy(q: Query[(DeploymentRequestTable, ProductTable, Rep[Option[OperationTraceTable]], Rep[Option[ExecutionTraceTable]], Rep[Option[TargetStatusTable]]), DeepRecords, Seq],
                        order: Seq[Map[String, Any]]) = {
    order.foldRight(q.sortBy(_._1.id)) { (spec, queries) =>
      val descending = try spec.getOrElse("desc", false).asInstanceOf[Boolean].value catch {
        case _: ClassCastException => throw new IllegalArgumentException("Orders `desc` must be true or false")
      }
      val fieldName = spec.getOrElse("field", throw new IllegalArgumentException(s"Orders must specify ̀`field`"))
      fieldName match {
        case "creationDate" => queries.sortBy(if (descending) _._1.creationDate.desc else _._1.creationDate.asc)
        case "creator" => queries.sortBy(if (descending) _._1.creator.desc else _._1.creator.asc)
        case "productName" => queries.sortBy(if (descending) _._2.name.desc else _._2.name.asc)
        case _ => throw new IllegalArgumentException(s"Cannot sort by `$fieldName`")
      }
    }
  }

  // >>

  // todo: when the target status will be pre-registered, we won't need to get execution traces in this query anymore (and we will only need the last operation)
  def deepQueryDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[(DeepDeploymentRequest, Option[OperationEffect])]] = {
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

    deepQueryDeploymentRequests(
      sortBy(filtered, orderBy)
        drop offset
        take limit,
      orderBy
    )
  }

  private def deepQueryDeploymentRequests(q: Query[(DeploymentRequestTable, ProductTable), (DeploymentRequestRecord, ProductRecord), scala.Seq],
                                          order: Seq[Map[String, Any]]): Future[Iterable[(DeepDeploymentRequest, Option[OperationEffect])]] = {
    type StableMap = LinkedHashMap[Long, ArrayBuffer[DeepRecords]]

    def groupByDeploymentRequestId(x: Iterable[DeepRecords]): StableMap = {
      x.foldLeft(new StableMap()) { case (result, records) =>
        result.getOrElseUpdate(records._1.id.get, new ArrayBuffer[DeepRecords]()).append(records)
        result
      }
    }

    dbContext.db.run(
      sortAllBy(
        q
          // get the last operation for each deployment request in the provided query
          .joinLeft(operationTraceQuery).on(_._1.id === _.deploymentRequestId)
          .groupBy { case ((request, _), _) => request.id }
          .map { case (requestId, operations) => (requestId, operations.map { case (_, operation) => operation.map(_.id) }.max) }

          // since we lost everything but the selected operation trace ID (because of Slick and SQL backend limitations), let's get everything back
          .join(productQuery)
          .join(deploymentRequestQuery)
          .filter { case (((requestId, _), product), request) => requestId === request.id && request.productId === product.id }
          .joinLeft(operationTraceQuery).on { case ((((_, operationId), _), _), operation) => operationId === operation.id }

          // get the execution traces and target statuses related to each of these operations
          .joinLeft(executionQuery).on { case ((_, operation), execution) => operation.map(_.id) === execution.operationTraceId }
          .joinLeft(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
          .joinLeft(targetStatusQuery).on { case (((_, execution), _), targetStatus) => execution.map(_.id) === targetStatus.executionId }
          .map { case ((((((_, product), deploymentRequest), operationTrace), _), executionTrace), targetStatus) => (deploymentRequest, product, operationTrace, executionTrace, targetStatus) }
        , order).result
    ).map(groupByDeploymentRequestId(_).values.map { records =>
      val (request, operations) = oneRequestRecordsToDeepModel(records)
      assert(operations.isEmpty || operations.tail.isEmpty)
      (request, operations.headOption)
    })
  }

  // todo: should rely on a nullable field `startDate` in OperationTrace
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

  def isOutdated(deploymentRequest: DeploymentRequest): Future[Boolean] = {
    val outdatedByOperation =
      deploymentRequestQuery
        .filter(depReq => depReq.id > deploymentRequest.id && depReq.productId === deploymentRequest.productId)
        .join(operationTraceQuery).on(_.id === _.deploymentRequestId)
        .exists // fixme: look at the starting date when it will make sense
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

  def findOperationEffect(operationTrace: OperationTrace): Future[Option[OperationEffect]] =
    dbContext.db
      .run(executionQuery
        .filter(_.operationTraceId === operationTrace.id)
        .joinLeft(executionTraceQuery).on { case (execution, executionTrace) => execution.id === executionTrace.executionId }
        .joinLeft(targetStatusQuery).on { case ((execution, _), targetStatus) => execution.id === targetStatus.executionId }
        .map { case ((_, executionTrace), targetStatus) => (executionTrace, targetStatus) }
        .result
      )
      .map(results =>
        results.headOption.map { _ => toOperationEffect(operationTrace, results) }
      )

  // fixme: early target resolution should make this function obsolete
  def findLastOperationAndEffect(productId: Int): Future[Option[OperationEffect]] = {
    dbContext.db.run(
      deploymentRequestQuery
        .filter(_.productId === productId)
        .join(operationTraceQuery).on { case (deploymentRequest, operationTrace) => deploymentRequest.id === operationTrace.deploymentRequestId }
        .sortBy { case (_, op) => op.id.desc }
        .take(1)
        .joinLeft(executionQuery).on { case ((_, operationTrace), execution) => operationTrace.id === execution.operationTraceId }
        .joinLeft(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
        .joinLeft(targetStatusQuery).on { case (((_, execution), _), targetStatus) => execution.map(_.id) === targetStatus.executionId }
        .map { case ((((_, operationTrace), _), executionTrace), targetStatus) => (operationTrace, executionTrace, targetStatus) }
        .result)

      .map(results =>
        results.headOption.map { case (op, _, _) =>
          val operationTrace = op.toOperationTrace
          toOperationEffect(operationTrace, results.map { case (_, e, t) => (e, t) })
        }
      )
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

  // TODO: remove <<
  private val queryUnreferencedProductIds =
    productQuery
      .joinLeft(deploymentRequestQuery).on { case (product, deploymentRequest) => product.id === deploymentRequest.productId }
      .filter { case (_, deploymentRequest) => deploymentRequest.isEmpty }
      .map { case (product, _) => product.id }

  def countUnreferencedProducts(): Future[Int] =
    dbContext.db.run(queryUnreferencedProductIds.countDistinct.result)

  def deleteUnreferencedProducts(): Future[Int] =
    dbContext.db.run(
      productQuery
        .filter(_.id.in(queryUnreferencedProductIds))
        .delete
    )

  // >>
}
