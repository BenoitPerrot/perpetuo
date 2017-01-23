package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.dispatchers.DeploymentRequestParser.parse
import com.criteo.perpetuo.dispatchers.{Execution, TargetDispatching}
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.exceptions.BadRequestException
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import spray.json.JsonParser.ParsingException

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try


@JsonIgnoreBody
case class DeploymentRequestGet(@RouteParam @NotEmpty id: String)


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val execution: Execution)
  extends BaseController {

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")

  get("/api/deployment-requests/:id") {
    r: DeploymentRequestGet =>
      futurePool {
        Try(r.id.toLong).toOption.flatMap(id => Await.result(execution.dbBinding.findDeploymentRequestById(id), 2.seconds)).map(_.toJsonReadyMap)
      }
  }

  post("/api/deployment-requests") {
    r: Request =>
      // parse the request
      val deploymentRequest = try {
        parse(r.contentString)
      } catch {
        case e: ParsingException => throw new BadRequestException(e.getMessage)
      }

      futurePool {
        // trigger the execution
        val (futureId, asyncStart) = execution.startTransaction(TargetDispatching.fromConfig, deploymentRequest)

        asyncStart.onFailure({ case e => logger.error("Transaction failed to start: " + e.getMessage + "\n" + e.getStackTrace.mkString("\n")) })

        // return the ID
        val id = Await.result(futureId, 2.seconds)
        response.created.json(Map("id" -> id))
      }
  }

}
