package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.app.RawJson
import slick.lifted.ColumnOrdered

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


class Schema(val dbContext: DbContext)
  extends ExecutionTraceBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  val all = productQuery.schema ++ deploymentRequestQuery.schema ++ operationTraceQuery.schema ++ executionTraceQuery.schema

  def createTables(): Unit = {
    Await.result(dbContext.db.run(all.create), 2.seconds)
  }
}


@Singleton
class DbBinding @Inject()(val dbContext: DbContext)
  extends ExecutionTraceBinder with OperationTraceBinder with DeploymentRequestBinder with ProductBinder
    with DbContextProvider {

  import dbContext.driver.api._

  def deepQueryDeploymentRequests(where: Seq[Map[String, Any]], orderBy: Seq[Map[String, Any]], limit: Int, offset: Int): Future[Iterable[Map[String, Object]]] = {

    val filtered = where.foldLeft(this.deploymentRequestQuery join this.productQuery on (_.productId === _.id)) { (queries, spec) =>
      val value = spec.getOrElse("equals", throw new IllegalArgumentException(s"Filters tests must be `equals`"))
      val fieldName = spec.getOrElse("field", throw new IllegalArgumentException(s"Filters must specify ̀`field`"))
      fieldName match {
        case "productName" => try queries.filter(_._2.name === value.asInstanceOf[String]) catch { case e: ClassCastException => throw new IllegalArgumentException("Filters on `productName` must test against a string value") }
        case _ => throw new IllegalArgumentException(s"Cannot filter on `$fieldName`")
      }
    }

    val filteredThenSorted = orderBy.reverse.foldLeft(filtered.sortBy (_._1.id)) { (queries, spec) =>
      val descending = try spec.getOrElse("desc", false).asInstanceOf[Boolean].value catch { case e: ClassCastException => throw new IllegalArgumentException("Orders `desc` must be true or false") }
      val fieldName = spec.getOrElse("field", throw new IllegalArgumentException(s"Orders must specify ̀`field`"))
      fieldName match {
        case "creationDate" => queries.sortBy(if (descending) _._1.creationDate.desc else _._1.creationDate.asc)
        case "creator" => queries.sortBy(if (descending) _._1.creator.desc else _._1.creator.asc)
        case "version" => queries.sortBy(if (descending) _._1.version.desc else _._1.version.asc)
        case "productName" => queries.sortBy(if (descending) _._2.name.desc else _._2.name.asc)
        case _ => throw new IllegalArgumentException(s"Cannot sort by `$fieldName`")
      }
    }

    type StableMap = mutable.LinkedHashMap[Long, (DeploymentRequestRecord, ProductRecord, ArrayBuffer[(Option[OperationTraceRecord], Option[ExecutionTraceRecord])])]

    def groupByDeploymentRequestId(x: Seq[(((DeploymentRequestRecord, ProductRecord), Option[OperationTraceRecord]), Option[ExecutionTraceRecord])]): StableMap = {
      x.foldLeft(new StableMap()) { case (result, (((deploymentRequest, product), operationTrace), executionTrace)) =>
        result.getOrElseUpdate(deploymentRequest.id.get, {
          (deploymentRequest, product, ArrayBuffer())
        })._3.append((operationTrace, executionTrace))
        result
      }
    }

    dbContext.db.run((
      filteredThenSorted
        drop offset
        take limit
        joinLeft operationTraceQuery on (_._1.id === _.deploymentRequestId)
        joinLeft executionTraceQuery on (_._2.map(_.id) === _.operationTraceId)).result)
      .map {
        groupByDeploymentRequestId(_).values.map { case (req, product, opAndExecs) =>
          val perOperationId = opAndExecs.collect {
            case (op, exec) if op.isDefined => (op.get, exec)
          }.groupBy(_._1.id)

          Map(
            "id" -> req.id,
            "comment" -> req.comment,
            "creationDate" -> req.creationDate,
            "creator" -> req.creator,
            "version" -> req.version,
            "target" -> RawJson(req.target),
            "productName" -> product.name,
            "operations" -> perOperationId.values.map { opAndExecs =>
              val (op, _) = opAndExecs.head
              val execs = opAndExecs.collect {
                case (_, exec) if exec.isDefined => exec.get
              }
              Map(
                "id" -> op.id.get,
                "type" -> op.operation.toString,
                "targetStatus" -> op.targetStatus.mapValues(s => Map("code" -> s.code.toString, "detail" -> s.detail)),
                "executions" -> execs.map(exec =>
                  Map(
                    "id" -> exec.id.get,
                    "logHref" -> exec.logHref,
                    "state" -> exec.state.toString
                  )
                )
              )
            }
          )
        }
      }
  }
}