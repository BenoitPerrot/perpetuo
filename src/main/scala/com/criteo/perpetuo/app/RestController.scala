package com.criteo.perpetuo.app

import java.sql.SQLException
import javax.inject.Inject

import com.criteo.perpetuo.dispatchers.{Execution, TargetDispatcher}
import com.criteo.perpetuo.model.DeploymentRequestParser.parse
import com.criteo.perpetuo.model.Product
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
        Await.result(execution.dbBinding.getProducts, 2.seconds).map(_.name)
      }
  }

  post("/api/products") {
    r: ProductPost =>
      futurePool {
        Await.result(execution.dbBinding.insert(Product(None, r.name)).recover {
          // todo: check the error when with SQL Server
          case e: SQLException if e.getMessage.startsWith("Unique index") =>
            throw BadRequestException(s"Name `${r.name}` is already used")
        }, 2.seconds)
        response.created.nothing
      }
  }

  get("/api/deployment-requests/:id")(
    withLongId(
      execution.dbBinding.findDeploymentRequestByIdAndProduct(_).map(_.map(_.toJsonReadyMap))
    )
  )

  post("/api/deployment-requests") {
    r: Request =>
      futurePool {
        Await.result(
          {
            val futureDepReq = try {
              parse(r.contentString, productName =>
                execution.dbBinding.findProductByName(productName).map(
                  _.getOrElse { throw BadRequestException(s"Product `$productName` could not be found") }
                )
              )
            }
            catch {
              case e: ParsingException => throw BadRequestException(e.getMessage)
            }

            futureDepReq
              .flatMap { deploymentRequest =>
                val (futureId, asyncStart) = execution.startTransaction(dispatcher, deploymentRequest)
                asyncStart.onFailure({ case e => logger.error("Transaction failed to start: " + e.getMessage + "\n" + e.getStackTrace.mkString("\n")) })
                futureId
              }
              .map { id => response.created.json(Map("id" -> id)) }
          },
          2.seconds
        )
      }
  }

}
