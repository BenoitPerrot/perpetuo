package com.criteo.perpetuo.app

import java.sql.Timestamp
import javax.inject.Inject
import javax.sql.DataSource

import com.criteo.perpetuo.dao.{DeploymentIntent, DeploymentRequest, DeploymentRequestBinding}
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

// this is not perfect but thanks to this case class, Finatra handles the input validation by itself
case class DeploymentRequestPost(@NotEmpty productName: String,
                                 @NotEmpty version: String,
                                 @NotEmpty target: String,
                                 reason: String = "") extends DeploymentIntent


/**
  * Controller that handles deployment requests as a REST API.
  */
class RestController @Inject()(val dataSource: DataSource,
                               val deploymentRequests: DeploymentRequestBinding)
  extends BaseController {

  import deploymentRequests.profile.api._

  val db = Database.forDataSource(dataSource)

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
      Try(r.id.toLong).toOption.flatMap(id => Await.result(deploymentRequests.findDeploymentRequestById(db, id), 2.seconds))
    // todo: remove "id" from output, as well as "reason" when it's empty?
  }

  post("/api/deployment-requests") {
    r: DeploymentRequestPost => {
      val req = DeploymentRequest(None, r.productName, r.version, r.target, r.reason, "anonymous", new Timestamp(System.currentTimeMillis))
      Await.result(deploymentRequests.insert(db, req).map(response.created.json), 2.seconds)
    }
  }
}
