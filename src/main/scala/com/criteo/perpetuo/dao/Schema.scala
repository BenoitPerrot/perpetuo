package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.model._

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap => MutableMap}
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

  def removeOldFks() = {
    dbContext.db.run(executionSpecificationQuery.filter(_.operationTraceId.nonEmpty).map(_.operationTraceId).update(None))
    dbContext.db.run(targetStatusQuery.filter(_.operationTraceId.nonEmpty).map(ts => (ts.operationTraceId, ts.executionSpecificationId)).update((None, None)))
  }

  def countOldFks() = {
    dbContext.db.run(executionSpecificationQuery.filter(_.operationTraceId.nonEmpty).length.result)
    dbContext.db.run(targetStatusQuery.filter(_.operationTraceId.nonEmpty).length.result)
  }

  def setExecutionTracesMissingDetails() =
    dbContext.db.run(
      sqlu"""
        UPDATE execution_trace
        SET execution_trace.detail = target_status.detail
        FROM execution_trace JOIN target_status ON execution_trace.execution_id = target_status.execution_id
        WHERE execution_trace.state = 3 AND execution_trace.detail = '' AND target_status.detail != ''
      """)

  def countExecutionTracesMissingDetails() =
    dbContext.db.run(
      executionTraceQuery.join(targetStatusQuery)
        .filter { case (trace, status) =>
          trace.executionId === status.executionId &&
            trace.state === ExecutionState.initFailed && trace.detail === "" && status.detail =!= ""
        }
        .length
        .result)

  def removeInitFailureDetails() =
    dbContext.db.run(
      sqlu"""
        DELETE target_status
        FROM target_status JOIN execution_trace ON execution_trace.execution_id = target_status.execution_id
        WHERE execution_trace.state = 3
      """
    )

  def countInitFailureDetails() =
    dbContext.db.run(
      targetStatusQuery.join(executionTraceQuery)
        .filter { case (status, trace) =>
          status.executionId === trace.executionId &&
            trace.state === ExecutionState.initFailed
        }
        .length
        .result)
}


@Singleton
class DbBinding @Inject()(val dbContext: DbContext)
  extends ExecutionTraceBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with TargetStatusBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  def findDeepDeploymentRequestAndExecutions(deploymentRequestId: Long): Future[Option[(DeepDeploymentRequest, Iterable[(Iterable[ExecutionTrace], Iterable[TargetStatus])])]] = {
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

      .map(results =>
        results.headOption.map { case (deploymentRequestRecord, productRecord, _, _, _) =>
          val deploymentRequest = deploymentRequestRecord.toDeepDeploymentRequest(productRecord)

          val sortedGroupsOfExecutionsAndResults = results
            .toStream
            .collect { case (_, _, operationTrace, executionTrace, targetStatus) if operationTrace.isDefined =>
              (operationTrace, executionTrace, targetStatus)
            }
            .groupBy { case (operationTrace, _, _) => operationTrace.get.id.get }
            .toStream
            .sortBy(_._1)
            .map { case (_, l) =>
              val (operationTraceRecord, _, _) = l.head
              val operationTrace = operationTraceRecord.get.toOperationTrace
              val executionTraces = l.map(_._2).filter(_.isDefined).map(_.get).distinct.map(_.toExecutionTrace(operationTrace))
              val targetStatuses = l.map(_._3).filter(_.isDefined).map(_.get).distinct.map(_.toTargetStatus)
              (executionTraces, targetStatuses)
            }
            .filter { case (et, _) => et.nonEmpty }

          (deploymentRequest, sortedGroupsOfExecutionsAndResults)
        }
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
  def sortBy(q: Query[(DeploymentRequestTable, ProductTable), (DeploymentRequestRecord, ProductRecord), scala.Seq], order: Seq[Map[String, Any]]) = {
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

  def sortAllBy(q: Query[(DeploymentRequestTable, ProductTable, Rep[Option[OperationTraceTable]], Rep[Option[ExecutionTraceTable]]), (DeploymentRequestRecord, ProductRecord, Option[OperationTraceRecord], Option[ExecutionTraceRecord]), scala.Seq],
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
  def deepQueryDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[(DeepDeploymentRequest, Iterable[ArrayBuffer[ExecutionTrace]])]] = {

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
                                          order: Seq[Map[String, Any]]): Future[Iterable[(DeepDeploymentRequest, Iterable[ArrayBuffer[ExecutionTrace]])]] = {
    type StableMap = MutableMap[Long, (DeploymentRequestRecord, ProductRecord, ArrayBuffer[ExecutionTrace])]

    def groupByDeploymentRequestId(x: Seq[(DeploymentRequestRecord, ProductRecord, Option[OperationTraceRecord], Option[ExecutionTraceRecord])]): StableMap = {
      x.foldLeft(new StableMap()) { case (result, (deploymentRequest, product, operationTrace, executionTrace)) =>
        val execs = result.getOrElseUpdate(deploymentRequest.id.get, {
          (deploymentRequest, product, ArrayBuffer())
        })._3
        executionTrace.foreach(e => execs.append(e.toExecutionTrace(operationTrace.get.toOperationTrace)))
        result
      }
    }

    dbContext.db.run(
      sortAllBy(
        q
          .joinLeft(operationTraceQuery).on(_._1.id === _.deploymentRequestId)
          .joinLeft(executionQuery).on { case ((_, operationTrace), execution) => operationTrace.map(_.id) === execution.operationTraceId }
          .joinLeft(executionTraceQuery).on { case ((_, execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
          .map { case ((((deploymentRequest, product), operationTrace), _), executionTrace) => (deploymentRequest, product, operationTrace, executionTrace) }
        , order).result
    ).map(groupByDeploymentRequestId(_).values.map { case (req, product, execs) =>
      (
        req.toDeepDeploymentRequest(product),
        execs.groupBy(_.operationTrace.id).toStream.sortBy(_._1).map(_._2)
      )
    })
  }

  /**
    * @return - None if the operation doesn't exist or is still running,
    *         - Some(True) if it's closed and all targets are marked successful,
    *         - Some(False) if it's closed and at least one target is not successful
    */
  def isOperationSuccessful(id: Long): Future[Option[Boolean]] = {
    dbContext.db.run(
      operationTraceQuery.filter(op => op.id === id && op.closingDate.isDefined)
        .joinLeft(
          executionQuery.join(targetStatusQuery).on(_.id === _.executionId)
            .filter(_._2.code =!= Status.success)
            .map { case (execution, _) => execution }
        ).on(_.id === _.operationTraceId)
        .take(1)
        .map { case (_, execution) => execution }
        .result
    ).map(_.headOption.map(_.isEmpty))
  }

  // todo: should rely on a nullable field `startDate` in OperationTrace
  def isDeploymentRequestStarted(deploymentRequestId: Long): Future[Option[(ShallowDeploymentRequest, Boolean)]] = {
    dbContext.db.run(
      deploymentRequestQuery
        .filter(_.id === deploymentRequestId)
        .joinLeft(operationTraceQuery).on(_.id === _.deploymentRequestId)
        .map { case (depReq, op) => (depReq, op.isDefined) }
        .result
    ).map(_.headOption.map { case (depReq, started) => (depReq.toShallowDeploymentRequest, started) })
  }

  def isOutdated(deploymentRequest: DeploymentRequest): Future[Boolean] = {
    val outdatedByOperation =
      deploymentRequestQuery
        .filter(depReq => depReq.id > deploymentRequest.id && depReq.productId === deploymentRequest.productId)
        .join(operationTraceQuery).on(_.id === _.deploymentRequestId)
        .exists // fixme: look at the starting date when it will make sense
    dbContext.db.run(outdatedByOperation.result)
  }

  private def targetAtomsQuery(deploymentRequest: DeploymentRequest) =
    operationTraceQuery.filter(_.deploymentRequestId === deploymentRequest.id).take(1)
      .join(executionQuery)
      .join(targetStatusQuery)
      .filter { case ((operationTrace, execution), targetStatus) =>
        operationTrace.id === execution.operationTraceId && execution.id === targetStatus.executionId
      }
      .map { case (_, targetStatus) => targetStatus.targetAtom }

  def findExecutionSpecificationsForRollback(deploymentRequest: DeploymentRequest): Future[Map[TargetAtom.Type, Option[ExecutionSpecification]]] = {
    val previousTargetStatuses = targetStatusQuery
      .join(executionQuery)
      .join(operationTraceQuery)
      .join(deploymentRequestQuery)
      .filter { case (((targetStatus, execution), operationTrace), oldDeploymentRequest) =>
        targetStatus.executionId === execution.id && execution.operationTraceId === operationTrace.id && operationTrace.deploymentRequestId === oldDeploymentRequest.id &&
          oldDeploymentRequest.productId === deploymentRequest.productId && oldDeploymentRequest.id < deploymentRequest.id
        // because it's impossible to apply deployment requests in another order than creation one
      }
      .map { case (((targetStatus, _), _), _) => targetStatus }

    val lastExecutionIdPerTarget = targetAtomsQuery(deploymentRequest)
      .joinLeft(previousTargetStatuses)
      .on { case (impactedTargetAtom, anyTargetStatus) => impactedTargetAtom === anyTargetStatus.targetAtom }
      .groupBy { case (targetAtom, _) => targetAtom }
      .map { case (targetAtom, q) =>
        (targetAtom, q.map { case (_, targetStatus) => targetStatus.map(_.executionId) }.max)
      }

    val execSpecIds = lastExecutionIdPerTarget
      .joinLeft(
        executionQuery.join(executionSpecificationQuery).on(_.executionSpecificationId === _.id)
          .map { case (execution, execSpec) => (execution.id, execSpec) }
      )
      .on { case ((_, targetExecutionId), (anyExecutionId, _)) => targetExecutionId === anyExecutionId }
      .map { case ((targetAtom, _), specLink) => (targetAtom, specLink) }

    dbContext.db.run(execSpecIds.result).map { seq =>
      val res = seq.toStream.map { case (targetAtom, execSpec) =>
        (targetAtom, execSpec.map { case (_, spec) => spec.toExecutionSpecification })
      }.toMap
      assert(res.size == seq.size)
      res
    }
  }

  def findTargetAtomNotActionableBy(deploymentRequest: DeploymentRequest): Future[Option[TargetAtom.Type]] = {
    // The given deployment request has a specification for each target atom;
    // once we put aside the possible revert operations on this very deployment request (only),
    // for each target atom, if the last operation has the same specification as this deployment request,
    // then the deployment request is actionable: it can be retried, it can be rolled back.
    // Note that if this deployment request has never been applied, it's also actionable.
    val query = operationTraceQuery
      .filter { op => op.deploymentRequestId === deploymentRequest.id && op.operation === Operation.deploy }
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
      .filter { case ((((operationTrace, (targetAtom, lastExecutionId)), testedExecution), targetStatus), execution) =>
        operationTrace.id === testedExecution.operationTraceId && testedExecution.id === targetStatus.executionId &&
          targetStatus.targetAtom === targetAtom && lastExecutionId === execution.id
      }
      .map { case ((((_, (targetAtom, _)), testedExecution), _), lastExecution) =>
        (targetAtom, testedExecution.executionSpecificationId === lastExecution.executionSpecificationId)
      }

    dbContext.db.run(query.result)
      .map(_.collectFirst { case (targetAtom, actionable) if !actionable => targetAtom })
  }
}
