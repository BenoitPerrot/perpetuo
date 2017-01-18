package com.criteo.perpetuo.app

import java.lang.reflect.Modifier
import javax.inject.Inject
import javax.sql.DataSource

import com.criteo.perpetuo.dao.{DeploymentRequest, DeploymentRequestBinding, Schema}
import com.criteo.perpetuo.dispatchers.DeploymentRequestParser.parse
import com.criteo.perpetuo.dispatchers.{Execution, TargetDispatching}
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.exceptions.BadRequestException
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.utils.FuturePools
import com.twitter.finatra.validation._
import slick.driver.H2Driver
import spray.json.JsonParser.ParsingException

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try


@JsonIgnoreBody
case class DeploymentRequestGet(@RouteParam @NotEmpty id: String)


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val dataSource: DataSource,
                               val execution: Execution,
                               val deploymentRequests: DeploymentRequestBinding)
  extends BaseController {

  import deploymentRequests.profile.api._

  private val futurePool = FuturePools.unboundedPool("RequestFuturePool")
  private val db = Database.forDataSource(dataSource)

  if (deploymentRequests.profile.isInstanceOf[H2Driver]) {
    // running in development mode
    new Schema(deploymentRequests.profile).createTables(db)
  }

  get("/api/deployment-requests/:id") {
    r: DeploymentRequestGet =>
      futurePool {
        Try(r.id.toLong).toOption.flatMap(id => Await.result(deploymentRequests.findDeploymentRequestById(db, id), 2.seconds)).map {
          depReq =>
            val cls = classOf[DeploymentRequest]
            cls.getDeclaredFields
              .filterNot(_.isSynthetic)
              .map(_.getName)
              .map(cls.getDeclaredMethod(_))
              .filterNot(method => Modifier.isPrivate(method.getModifiers))
              .flatMap(method =>
                (method.getName, method.invoke(depReq)) match {
                  case ("reason", "") => None
                  case ("target", json: String) => Some("target" -> RawJson(json))
                  case (name, value) => Some(name -> value)
                }
              ).toMap
        }
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
        val (futureId, _) = execution.startTransaction(db, TargetDispatching.fromConfig, deploymentRequest)

        // return the ID
        val id = Await.result(futureId, 2.seconds)
        response.created.json(Map("id" -> id))
      }
  }

}
