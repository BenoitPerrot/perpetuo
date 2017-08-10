package com.criteo.perpetuo.dao

import java.net.InetSocketAddress
import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.app.RawJson
import com.criteo.perpetuo.model._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http, Request, Status => HttpStatus}
import com.twitter.util.{Await => TwitterAwait}
import spray.json._

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

  // TODO: remove once migration done
  // <<
  def setMissingTargetStatuses(): Future[Int] = {
    listExecutionsIdsMissingTargetStatus().map(_.grouped(100).flatMap {
      batch =>
        Await.result(
          Future.sequence {
            batch.map { case (execId, targetStatus) =>
              var statusMap = targetStatus.parseJson.asJsObject.fields.mapValues { value =>
                val Vector(JsNumber(statusId), JsString(detail)) = value.asInstanceOf[JsArray].elements
                TargetAtomStatus(Status(statusId.toInt), detail)
              }
              if (statusMap.isEmpty)
                statusMap = Map("*" -> TargetAtomStatus(Status.hostFailure, "The job has been killed"))
              insertTargetStatuses(execId, statusMap)
            }
          },
          5.minutes
        )
    }.length)
  }

  def countMissingTargetStatuses(): Future[Int] = {
    listExecutionsIdsMissingTargetStatus().map(_.length)
  }

  def listExecutionsIdsMissingTargetStatus(): Future[Vector[(Long, String)]] = {
    dbContext.db.run(
      sql"""select execution.id, operation_trace.target_status from execution join operation_trace on operation_trace_id = operation_trace.id
            where (select count(*) from target_status where execution_id = execution.id) = 0;""".as[(Long, String)]
    )
  }
  // >>
}


@Singleton
class DbBinding @Inject()(val dbContext: DbContext)
  extends ExecutionTraceBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with TargetStatusBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  def deepQueryDeploymentRequests(id: Long): Future[Option[(DeploymentRequest, SortedMap[Long, ArrayBuffer[ExecutionTrace]])]] = {
    deepQueryDeploymentRequests(
      deploymentRequestQuery
        filter { _.id === id }
        join productQuery on (_.productId === _.id),
      Seq()
    ).map { _.headOption }
  }

  def findOperationTraceAndExecutionSpecs(operationTraceId: Long): Future[Option[(OperationTrace, Seq[ExecutionSpecification])]] = {
    dbContext.db.run(operationTraceQuery
      .join(executionQuery)
      .join(executionSpecificationQuery)
      .filter { case ((operationTrace, execution), executionSpec) =>
        operationTrace.id === operationTraceId &&
          execution.operationTraceId === operationTraceId &&
          execution.executionSpecificationId === executionSpec.id
      }.map { case ((operationTrace, _), executionSpec) =>
          (operationTrace, executionSpec)
      }.result
    ).map(allOperationTraceAndExecutionSpecs =>
      allOperationTraceAndExecutionSpecs.headOption.map(x => (x._1.toOperationTrace, allOperationTraceAndExecutionSpecs.map(_._2.toExecutionSpecification)))
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
        case "version" => queries.sortBy(if (descending) _._1.version.desc else _._1.version.asc)
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
        case "version" => queries.sortBy(if (descending) _._1.version.desc else _._1.version.asc)
        case "productName" => queries.sortBy(if (descending) _._2.name.desc else _._2.name.asc)
        case _ => throw new IllegalArgumentException(s"Cannot sort by `$fieldName`")
      }
    }
  }
  // >>

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
}
