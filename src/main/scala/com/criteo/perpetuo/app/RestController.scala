package com.criteo.perpetuo.app

import javax.inject.Inject
import javax.sql.DataSource

import com.criteo.perpetuo.dao.{DeploymentRequest, DeploymentRequestBinding}
import com.criteo.perpetuo.dispatchers.DeploymentRequestParser.parse
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request._
import com.twitter.finatra.validation._
import slick.driver.H2Driver

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Try


@JsonIgnoreBody
case class DeploymentRequestGet(@RouteParam @NotEmpty id: String)


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val dataSource: DataSource,
                               val deploymentRequests: DeploymentRequestBinding)
  extends BaseController {

  import deploymentRequests.profile.api._

  private val db = Database.forDataSource(dataSource)

  {
    // TODO: Remove once live
    val isEmbedded = deploymentRequests.profile.isInstanceOf[H2Driver]
    if (isEmbedded) {
      val schemaCreation = DBIO.seq(
        deploymentRequests.deploymentRequestQuery.schema.create
      )
      Await.result(db.run(schemaCreation), 2.second)
    }
  }

  get("/api/deployment-requests/:id") {
    r: DeploymentRequestGet =>
      Try(r.id.toLong).toOption.flatMap(id => Await.result(deploymentRequests.findDeploymentRequestById(db, id), 2.seconds)).map {
        depReq => {
          val cls = classOf[DeploymentRequest]
          cls.getDeclaredFields.filterNot(_.isSynthetic).map(_.getName).flatMap(fieldName =>
            (fieldName, cls.getDeclaredMethod(fieldName).invoke(depReq)) match {
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
      Await.result(deploymentRequests.insert(db, parse(r.contentString)._1).map(response.created.json), 2.seconds)
  }
}
