package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.dispatchers.{Execution, TargetDispatcher}
import com.criteo.perpetuo.model.DeploymentRequestParser.parse
import com.criteo.perpetuo.model.{DeploymentRequest, Product}
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
case class DeploymentRequestGet(@RouteParam @NotEmpty id: String)


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val execution: Execution)
  extends BaseController {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")
  private val dispatcher = TargetDispatcher.fromConfig

  private def findDeploymentRequestAndProduct(id: String): Future[Option[(DeploymentRequest, Product)]] = {
    Try(id.toLong).toOption
      .map(id => execution.dbBinding.findDeploymentRequestByIdAndProduct(id))
      .getOrElse(Future(None))
  }

  get("/api/deployment-requests/:id") {
    r: DeploymentRequestGet =>
      futurePool {
        Await.result(findDeploymentRequestAndProduct(r.id), 2.seconds).map(x => x._1.toJsonReadyMap(x._2))
      }
  }

  post("/api/deployment-requests") {
    r: Request =>
      futurePool {
        Await.result(
          parse(r.contentString, productName => { execution.dbBinding.findProductByName(productName).map {
            _.getOrElse { throw new BadRequestException(s"Product $productName could not be found") }
          } })
            .recover {
              case e: ParsingException => throw new BadRequestException(e.getMessage)
            }
            .flatMap { deploymentRequest =>
              val (futureId, asyncStart) = execution.startTransaction(dispatcher, deploymentRequest)
              asyncStart.onFailure({ case e => logger.error("Transaction failed to start: " + e.getMessage + "\n" + e.getStackTrace.mkString("\n")) })
              futureId
            }
            .map { id =>
              response.created.json(Map("id" -> id))
            },
          2.seconds)
      }
  }

}
