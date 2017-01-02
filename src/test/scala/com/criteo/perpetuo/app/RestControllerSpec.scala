package com.criteo.perpetuo.app

import com.twitter.finagle.http.Status.{Created, NotFound, Ok}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest
import com.typesafe.config.{Config, ConfigFactory}
import spray.json._


/**
  * An integration test for [[RestController]].
  */
class RestControllerSpec extends FeatureTest {

  val server = new EmbeddedHttpServer(new HttpServer {

    val config: Config = ConfigFactory.load()

    override protected def jacksonModule = CustomServerModules.jackson

    override def modules = Seq(
      new DbContextModule(config.getConfig("db").getConfig("embedded"))
    )

    override def configureHttp(router: HttpRouter) {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .add[RestController]
    }
  })

  "A Server" should {

    "return 404 when trying to access a non-existing DeploymentRequest" in {
      server.httpGet(
        path = "/api/deployment-requests/4242",
        andExpect = NotFound
      )
    }

    "return 404 when trying to access a DeploymentRequest with a non-integral ID" in {
      server.httpGet(
        path = "/api/deployment-requests/..",
        andExpect = NotFound
      )
    }

    def requestDeployment(productName: String, version: String, target: String, reason: Option[String]) =
      server.httpPost(
        path = "/api/deployment-requests",
        andExpect = Created,
        postBody = JsObject(
          Map(
            "productName" -> JsString(productName),
            "version" -> JsString(version),
            "target" -> JsString(target)
          ) ++ (if (reason.isDefined) Map("reason" -> JsString(reason.get)) else Map())
        ).compactPrint
      ).contentString.toLong

    "return 201 when creating a DeploymentRequest" in {
      requestDeployment("my product", "v21", "to everywhere", Some("my comment"))
    }

    "return 201 and a JSON with all necessary info when accessing an existing DeploymentRequest" in {
      val values = server.httpGet(
        path = "/api/deployment-requests/1",
        andExpect = Ok
      ).contentString.parseJson.asJsObject.fields
      values should contain allOf(
        "productName" -> JsString("my product"),
        "version" -> JsString("v21"),
        "target" -> JsString("to everywhere"),
        "reason" -> JsString("my comment"),
        "creator" -> JsString("anonymous")
      )
      values should contain key "creationDate"
      values("creationDate").toString.toLong should (be > 14e8.toLong and be < 20e8.toLong) // it's a timestamp in s.
    }

    "return 201 when creating a DeploymentRequest without explicit reason" in {
      requestDeployment("my other product", "buggy", "nowhere", None)
    }

  }
}
