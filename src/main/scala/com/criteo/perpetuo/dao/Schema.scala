package com.criteo.perpetuo.dao

import javax.inject.{Inject, Singleton}

import com.criteo.perpetuo.app.RawJson

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

  def deepQueryDeploymentRequests(): Future[Iterable[Map[String, Object]]] = {
    dbContext.db.run((
      deploymentRequestQuery
        join productQuery on (_.productId === _.id)
        joinLeft operationTraceQuery on (_._1.id === _.deploymentRequestId)
        joinLeft executionTraceQuery on (_._2.map(_.id) === _.operationTraceId)).result)
      .map(_.groupBy {
        case (((req, _), _), _) => req.id
      }.values.map { seq =>
        val (((req, prod), _), _) = seq.head
        val perOperationId = seq.collect {
          case (((_, _), op), exec) if op.isDefined => (op.get, exec)
        }.groupBy(_._1.id)

        Map(
          "id" -> req.id,
          "comment" -> req.comment,
          "creationDate" -> req.creationDate,
          "creator" -> req.creator,
          "version" -> req.version,
          "target" -> RawJson(req.target),
          "productName" -> prod.name,
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
      })
  }
}
