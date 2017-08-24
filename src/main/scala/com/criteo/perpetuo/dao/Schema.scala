package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.model._
import com.twitter.finagle.http.{Status => HttpStatus}
import com.twitter.util.{Await => TwitterAwait}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedMap, breakOut, mutable}
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
  extends ExecutionTraceBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with TargetStatusBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  def findDeepDeploymentRequest(deploymentRequestId: Long): Future[Option[(DeploymentRequest, SortedMap[Long, (Iterable[ExecutionTrace], Iterable[TargetStatus])])]] = {
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
          val deploymentRequest = deploymentRequestRecord.toDeploymentRequest(productRecord.toProduct)

          val executionResults = results
            .map { case (_, _, operationTrace, executionTrace, targetStatus) => (operationTrace, executionTrace, targetStatus) }
            .filter { case (operationTrace, executionTrace, targetStatus) => operationTrace.isDefined }
            .groupBy { case (operationTrace, executionTrace, targetStatus) => operationTrace.get.id.get }
            .mapValues { l =>
              val (operationTraceRecord, _, _) = l.head
              val operationTrace = operationTraceRecord.get.toOperationTrace
              val executionTraces = l.map(_._2).filter(_.isDefined).map(_.get).distinct.map(_.toExecutionTrace(operationTrace))
              val targetStatuses = l.map(_._3).filter(_.isDefined).map(_.get).distinct.map(_.toTargetStatus)
              (executionTraces, targetStatuses)
            }

          (deploymentRequest, SortedMap(executionResults.toSeq: _*))
        }
      )
  }

  def findDeploymentRequestAndSpecs(deploymentRequestId: Long): Future[Option[(DeploymentRequest, Seq[ExecutionSpecification])]] = {
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
        (deploymentRequest.toDeploymentRequest(product.toProduct), depReqAndExecSpecs.map(_._3.toExecutionSpecification))
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
  def deepQueryDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[(DeploymentRequest, SortedMap[Long, ArrayBuffer[ExecutionTrace]])]] = {

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
                                          order: Seq[Map[String, Any]]): Future[Iterable[(DeploymentRequest, SortedMap[Long, ArrayBuffer[ExecutionTrace]])]] = {
    type StableMap = mutable.LinkedHashMap[Long, (DeploymentRequestRecord, ProductRecord, ArrayBuffer[ExecutionTrace])]

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
          .joinLeft(executionTraceQuery).on { case (((_, operationTrace), execution), executionTrace) => execution.map(_.id) === executionTrace.executionId }
          .map { case ((((deploymentRequest, product), operationTrace), execution), executionTrace) => (deploymentRequest, product, operationTrace, executionTrace) }
        , order).result
    ).map(groupByDeploymentRequestId(_).values.map { case (req, product, execs) =>
      (req.toDeploymentRequest(product.toProduct), SortedMap(execs.groupBy(_.operationTrace.id).toStream: _*))
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
            .filter(_._2.code =!= Status.success).take(1).map(_._1)
        ).on(_.id === _.operationTraceId).map(_._2).result).map(
      _.headOption.map(_.isEmpty)
    )
  }

  private def findTargetAtomsForOperation(o: OperationTrace): Future[Seq[TargetAtom.Type]] = {
    val queryTargetAtomsForOperation =
      executionQuery
        .join(targetStatusQuery)
        .filter { case (execution, targetStatus) => execution.operationTraceId === o.id && execution.id === targetStatus.executionId }
        .map { case (_, targetStatus) => targetStatus.targetAtom }
    dbContext.db.run(queryTargetAtomsForOperation.result)
  }

  private def findSoundExecutionSpecIdsForRollback(o: OperationTrace): Future[Map[TargetAtom.Type, ExecutionSpecificationRecord]] = {
    val productIdAndTargetAtomsForOperation =
      executionQuery
        .join(targetStatusQuery)
        .join(deploymentRequestQuery)
        .filter { case ((execution, targetStatus), deploymentRequest) =>
          execution.operationTraceId === o.id && execution.id === targetStatus.executionId && deploymentRequest.id === o.deploymentRequestId
        }
        .map { case ((_, targetStatus), deploymentRequest) => (deploymentRequest.productId, targetStatus.targetAtom) }

    val executionIds =
      productIdAndTargetAtomsForOperation
        .join(targetStatusQuery)
        .join(executionQuery)
        .join(operationTraceQuery)
        .join(deploymentRequestQuery)
        .filter { case (((((productId, targetAtom), targetStatus), execution), operationTrace), deploymentRequest) =>
          targetStatus.executionId === execution.id && execution.operationTraceId === operationTrace.id && operationTrace.deploymentRequestId === deploymentRequest.id &&
            targetStatus.targetAtom === targetAtom && targetStatus.code === Status.success && deploymentRequest.productId === productId &&
            operationTrace.closingDate.map(_ < o.creationDate)
        }
        .map { case (((((_, targetAtom), _), execution), _), _) => (targetAtom, execution.id) }
        .groupBy { case (targetAtom, _) => targetAtom }
        .map { case (targetAtom, q) => (targetAtom, q.map { case (_, executionId) => executionId }.max) }
        .join(executionQuery)
        .join(executionSpecificationQuery)
        .filter { case (((_, executionId), execution), execSpec) => executionId.map(_ === execution.id) && execution.executionSpecificationId === execSpec.id }
        .map { case (((targetAtom, _), _), execSpec) => (targetAtom, execSpec) }

    dbContext.db.run(executionIds.result).map { seq =>
      val res = seq.toMap
      assert(res.size == seq.size)
      res
    }
  }

  def findExecutionSpecIdsForRollback(o: OperationTrace): Future[Map[TargetAtom.Type, Option[ExecutionSpecificationRecord]]] = {
    // TODO: find a way to do this in a single query instead of two
    val atoms = findTargetAtomsForOperation(o)
    findSoundExecutionSpecIdsForRollback(o).flatMap {
      specifications =>
        atoms.map(_.map(targetAtom => (targetAtom, specifications.get(targetAtom)))(breakOut))
    }
  }

  def findTargetAtomNotActionableBy(deploymentRequestId: Long): Future[Option[TargetAtom.Type]] = {
    // The given deployment request has a specification for each target atom;
    // once we put aside the possible revert operations on this very deployment request (only),
    // for each target atom, if the last operation has the same specification as this deployment request,
    // then the deployment request is actionable: it can be retried, it can be rolled back.
    // Note that if this deployment request has never been applied, it's also actionable.
    val query = operationTraceQuery
      .filter { op => op.deploymentRequestId === deploymentRequestId && op.operation === Operation.deploy }
      .take(1)
      .join(
        targetStatusQuery
          .join(executionQuery)
          .join(operationTraceQuery)
          .join(deploymentRequestQuery)
          .join(deploymentRequestQuery)
          .filter { case ((((targetStatus, execution), operationTrace), deploymentRequest), testedDeploymentRequest) =>
            targetStatus.executionId === execution.id && execution.operationTraceId === operationTrace.id &&
              operationTrace.deploymentRequestId === deploymentRequest.id && deploymentRequest.productId === testedDeploymentRequest.productId &&
              testedDeploymentRequest.id === deploymentRequestId &&
              !(operationTrace.operation === Operation.revert && deploymentRequest.id === deploymentRequestId)
          }
          .groupBy { case ((((targetStatus, _), _), _), _) => targetStatus.targetAtom }
          .map { case (targetAtom, q) => (targetAtom, q.map { case ((((_, execution), _), _), _) => execution.id }.max) }
      )
      .join(executionQuery)
      .join(targetStatusQuery)
      .join(executionQuery)
      .filter { case ((((operationTrace, (targetAtom, lastExecutionId)), testedExecution), targetStatus), execution) =>
        operationTrace.id === testedExecution.operationTraceId && testedExecution.id === targetStatus.executionId &&
          targetStatus.targetAtom === targetAtom && lastExecutionId.map(_ === execution.id).getOrElse(false)
      }
      .map { case ((((_, (targetAtom, _)), testedExecution), _), lastExecution) =>
        (targetAtom, testedExecution.executionSpecificationId === lastExecution.executionSpecificationId)
      }

    dbContext.db.run(query.result)
      .map(_.collectFirst { case (targetAtom, actionable) if !actionable => targetAtom })
  }
}
