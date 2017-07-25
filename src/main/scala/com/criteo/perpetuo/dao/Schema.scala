package com.criteo.perpetuo.dao

import java.net.InetSocketAddress
import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.app.RawJson
import com.criteo.perpetuo.model.ExecutionTrace
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Http, Request, Status}
import com.twitter.util.{Await => TwitterAwait}
import spray.json._

import scala.collection.mutable.ArrayBuffer
import scala.collection.{SortedMap, mutable}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class Schema(val dbContext: DbContext)
  extends LockBinder with ExecutionTraceBinder with TargetStatusBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  val all: dbContext.driver.DDL = productQuery.schema ++ deploymentRequestQuery.schema ++ operationTraceQuery.schema ++ executionSpecificationQuery.schema ++ executionQuery.schema ++ targetStatusQuery.schema ++ executionTraceQuery.schema ++ lockQuery.schema

  def createTables(): Unit = {
    Await.result(dbContext.db.run(all.create), 2.seconds)
  }

  // TODO: remove once migration done
  // <<
  def setOperationTracesMissingCreationDate() =
    // request is suboptimal, but it works on H2; hoping that sql compilers will be smart
    dbContext.db.run(sqlu"""
                        UPDATE operation_trace
                        SET creation_date = (
                            SELECT dr.creation_date
                            FROM deployment_request AS dr WHERE deployment_request_id = dr.id
                        )
                        WHERE creation_date = {ts '1970-01-01 00:00:00.000'}""")

  def countOperationTracesMissingCreationDate() =
    dbContext.db.run((operationTraceQuery filter (_.creationDate === new java.sql.Timestamp(0)) length).result)

  def setOperationTracesMissingClosingDate() =
    dbContext.db.run(sqlu"""
                        UPDATE operation_trace
                        SET closing_date = DATEADD(MINUTE, 1, (
                          SELECT dr.creation_date
                          FROM deployment_request AS dr WHERE deployment_request_id = dr.id)
                        )
                        WHERE closing_date IS NULL""")

  def countOperationTracesMissingClosingDate() =
    dbContext.db.run((operationTraceQuery filter (_.closingDate.isEmpty) length).result)

  def createAllExecutions(contentString: String): Future[Int] = {
    val productTypes: Map[String, String] = if (contentString.isEmpty)
      Map()
    else
      contentString.parseJson.asJsObject.fields.map { case (k, v) => k -> v.asInstanceOf[JsString].value }
    val client = ClientBuilder()
      .codec(Http())
      .hostConnectionLimit(3)
      .hosts(new InetSocketAddress("moab.criteois.lan", 80))
      .failFast(false)
      .build()
    dbContext.db.run(
      executionTraceQuery.filter(_.executionId.isEmpty)
        .join(operationTraceQuery).on(_.operationTraceId === _.id)
        .join(deploymentRequestQuery).on(_._2.deploymentRequestId === _.id)
        .join(productQuery).on(_._2.productId === _.id).result
    ).flatMap { seq =>
      val specParamsPerProduct = seq
        .map { case (_, product) => product.name }
        .toSet
        .flatMap { productName: String =>
          val productType: Option[String] = productTypes.get(productName).orElse {
            val request = Request(s"/products/$productName/manifest.json")
            request.headerMap.add("Host", "moab.criteois.lan")
            val resp = TwitterAwait.result(client.apply(request))
            println(resp.contentString)
            if (resp.status == Status.Ok)
              Some(resp.contentString.parseJson.asJsObject.fields("type").asInstanceOf[JsString].value)
            else
              None
          }
          productType.map(t => productName -> JsObject("jobName" -> JsString(s"deploy-to-$t")).toString)
        }
        .toMap

      Future.sequence(
        seq.flatMap { case (((execTrace, op), req), product) =>
          specParamsPerProduct.get(product.name).map { specParams =>
            val spec = ExecutionSpecificationRecord(None, Some(req.version), specParams)
            val insertions = for {
              specId <- (executionSpecificationQuery returning executionSpecificationQuery.map(_.id)) += spec
              execId <- (executionQuery returning executionQuery.map(_.id)) += ExecutionRecord(None, op.id.get, specId)
              updates <- executionTraceQuery.filter(_.id === execTrace.id.get).map(_.executionId).update(Some(execId))
            } yield updates
            dbContext.db.run(insertions.transactionally)
          }
        }
      )
    }.map(_.sum)
  }

  def countMissingExecutions(): Future[Int] =
    dbContext.db.run(executionTraceQuery.filter(_.executionId.isEmpty).length.result)

  // >>
}


@Singleton
class DbBinding @Inject()(val dbContext: DbContext)
  extends ExecutionTraceBinder with ExecutionBinder with ExecutionSpecificationBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  def deepQueryDeploymentRequests(id: Long): Future[Option[Map[String, Object]]] = {
    deepQueryDeploymentRequests(
      deploymentRequestQuery
        filter { _.id === id }
        join productQuery on (_.productId === _.id),
      Seq()
    ).map { _.headOption }
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

  def sortAllBy(q: Query[(((DeploymentRequestTable, ProductTable), Rep[Option[OperationTraceTable]]), Rep[Option[ExecutionTraceTable]]), (((DeploymentRequestRecord, ProductRecord), Option[OperationTraceRecord]), Option[ExecutionTraceRecord]), scala.Seq],
                order: Seq[Map[String, Any]]) = {
    order.foldRight(q.sortBy(_._1._1._1.id)) { (spec, queries) =>
      val descending = try spec.getOrElse("desc", false).asInstanceOf[Boolean].value catch {
        case _: ClassCastException => throw new IllegalArgumentException("Orders `desc` must be true or false")
      }
      val fieldName = spec.getOrElse("field", throw new IllegalArgumentException(s"Orders must specify ̀`field`"))
      fieldName match {
        case "creationDate" => queries.sortBy(if (descending) _._1._1._1.creationDate.desc else _._1._1._1.creationDate.asc)
        case "creator" => queries.sortBy(if (descending) _._1._1._1.creator.desc else _._1._1._1.creator.asc)
        case "version" => queries.sortBy(if (descending) _._1._1._1.version.desc else _._1._1._1.version.asc)
        case "productName" => queries.sortBy(if (descending) _._1._1._2.name.desc else _._1._1._2.name.asc)
        case _ => throw new IllegalArgumentException(s"Cannot sort by `$fieldName`")
      }
    }
  }
  // >>

  def deepQueryDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[Map[String, Object]]] = {

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

  private def deepQueryDeploymentRequests(q: Query[(DeploymentRequestTable, ProductTable), (DeploymentRequestRecord, ProductRecord), scala.Seq], order: Seq[Map[String, Any]]): Future[Iterable[Map[String, Object]]] = {

    type StableMap = mutable.LinkedHashMap[Long, (DeploymentRequestRecord, ProductRecord, ArrayBuffer[ExecutionTrace])]

    def groupByDeploymentRequestId(x: Seq[(((DeploymentRequestRecord, ProductRecord), Option[OperationTraceRecord]), Option[ExecutionTraceRecord])]): StableMap = {
      x.foldLeft(new StableMap()) { case (result, (((deploymentRequest, product), operationTrace), executionTrace)) =>
        val execs = result.getOrElseUpdate(deploymentRequest.id.get, {
          (deploymentRequest, product, ArrayBuffer())
        })._3
        executionTrace.foreach(e => execs.append(e.toExecutionTrace(operationTrace.get.toOperationTrace)))
        result
      }
    }

    dbContext.db.run(sortAllBy(q joinLeft operationTraceQuery on (_._1.id === _.deploymentRequestId) joinLeft executionTraceQuery on (_._2.map(_.id) === _.operationTraceId), order).result)
      .map {
        groupByDeploymentRequestId(_).values.map { case (req, product, execs) =>
          val sortedGroupsOfExecutions = SortedMap(execs.groupBy(_.operationTrace.id).toStream: _*).values

          Map(
            "id" -> req.id,
            "comment" -> req.comment,
            "creationDate" -> req.creationDate,
            "creator" -> req.creator,
            "version" -> req.version,
            "target" -> RawJson(req.target),
            "productName" -> product.name,
            "operations" -> sortedGroupsOfExecutions.map { execs =>
              val op = execs.head.operationTrace
              Map(
                "id" -> op.id,
                "type" -> op.operation.toString,
                "creator" -> op.creator,
                "creationDate" -> op.creationDate,
                "targetStatus" -> op.targetStatus,
                "executions" -> execs
              )
            }
          )
        }
      }
  }
}
