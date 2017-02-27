package com.criteo.perpetuo.app

import java.sql.SQLException
import javax.inject.Inject

import com.criteo.perpetuo.dao.UnknownProduct
import com.criteo.perpetuo.dispatchers.{Execution, TargetDispatcher}
import com.criteo.perpetuo.model.DeploymentRequestParser.parse
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.exceptions.BadRequestException
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import spray.json.JsonParser.ParsingException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try


@JsonIgnoreBody
private case class GetWithId(@RouteParam @NotEmpty id: String)


private case class ProductPost(@NotEmpty name: String)


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val execution: Execution)
  extends BaseController {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")
  private val dispatcher = TargetDispatcher.fromConfig

  private def withLongId[T](view: Long => Future[Option[T]]): GetWithId => com.twitter.util.Future[Option[T]] =
    request => futurePool {
      Try(request.id.toLong).toOption.map(view).flatMap(Await.result(_, 2.seconds))
    }


  get("/api/products") {
    _: Request =>
      futurePool {
        Await.result(execution.dbBinding.getProductNames, 2.seconds)
      }
  }

  post("/api/products") {
    r: ProductPost =>
      futurePool {
        Await.result(execution.dbBinding.insert(r.name).recover {
          case e: SQLException if e.getMessage.contains("nique index") =>
            // there is no specific exception type if the name is already used but the error message starts with
            // * if H2: Unique index or primary key violation: "ix_product_name ON PUBLIC.""product""(""name"") VALUES ('my product', 1)"
            // * if SQLServer: Cannot insert duplicate key row in object 'dbo.product' with unique index 'ix_product_name'
            throw BadRequestException(s"Name `${r.name}` is already used")
        }, 2.seconds)
        response.created.nothing
      }
  }

  get("/api/deployment-requests/:id")(
    withLongId(
      execution.dbBinding.findDeploymentRequestByIdWithProduct(_).map(_.map(_.toJsonReadyMap))
    )
  )

  post("/api/deployment-requests") {
    r: Request =>
      futurePool {
        Await.result(
          {
            val attrs = try {
              parse(r.contentString)
            }
            catch {
              case e: ParsingException => throw BadRequestException(e.getMessage)
            }

            val (futureDepReq, asyncStart) = execution.startTransaction(dispatcher, attrs)
            asyncStart.onFailure { case e => logger.error("Transaction failed to start: " + e.getMessage + "\n" + e.getStackTrace.mkString("\n")) }
            futureDepReq
              .recover { case e: UnknownProduct => throw BadRequestException(s"Product `${e.productName}` could not be found") }
              .map { depReq => response.created.json(Map("id" -> depReq.id)) }
          },
          2.seconds
        )
      }
  }

  get("/api/execution-traces/by-deployment-request/:id")(
    withLongId(id =>
      execution.dbBinding.findExecutionTracesByDeploymentRequest(id).flatMap { traces =>
        if (traces.isEmpty) {
          // if there is a deployment request with that ID, return the empty list, otherwise a 404
          execution.dbBinding.deploymentRequestExists(id).map(if (_) Some(traces) else None)
        }
        else
          Future.successful(
            Some(
              traces.map(trace =>
                Map(
                  "id" -> trace.id.get,
                  "logHref" -> trace.logHref,
                  "state" -> trace.state.toString
                )
              )
            )
          )
      }
    )
  )

  // Be sure to capture invalid calls to APIs
  get("/api/:*") { r: Request =>
    response.notFound
  }

}
