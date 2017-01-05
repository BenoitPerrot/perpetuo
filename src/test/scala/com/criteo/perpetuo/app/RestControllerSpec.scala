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

  private def requestDeployment(productName: String, version: String, target: String, reason: Option[String]): Long = {
    requestDeployment(productName, version, JsString(target), reason)
  }

  private def requestDeployment(productName: String, version: String, target: Iterable[String], reason: Option[String]): Long = {
    val targets = JsArray(target.map(s => JsString(s)).toVector)
    requestDeployment(productName, version, targets, reason)
  }

  private def requestDeployment(productName: String, version: String, target: JsValue, reason: Option[String]): Long =
    server.httpPost(
      path = "/api/deployment-requests",
      andExpect = Created,
      postBody = JsObject(
        Map(
          "productName" -> JsString(productName),
          "version" -> JsString(version),
          "target" -> target
        ) ++ (if (reason.isDefined) Map("reason" -> JsString(reason.get)) else Map())
      ).compactPrint
    ).contentString.toLong


  "The DeploymentRequest's POST entry-point" should {

    "returns 201 when creating a DeploymentRequest" in {
      requestDeployment("my product", "v21", "to everywhere", Some("my comment"))
    }

    "returns 201 when creating a DeploymentRequest without explicit reason" in {
      requestDeployment("my other product", "buggy", "nowhere", None)
    }

    "can handle a complex target expression" in {
      requestDeployment("my 3rd product", "42!", Seq("here", "and", "there"), Some(""))
    }

  }


  "The DeploymentRequest's GET entry-point" should {

    "returns 404 when trying to access a non-existing DeploymentRequest" in {
      server.httpGet(
        path = "/api/deployment-requests/4242",
        andExpect = NotFound
      )
    }

    "returns 404 when trying to access a DeploymentRequest with a non-integral ID" in {
      server.httpGet(
        path = "/api/deployment-requests/..",
        andExpect = NotFound
      )
    }

    "returns 200 and a JSON with all necessary info when accessing an existing DeploymentRequest" in {
      val values1 = server.httpGet(
        path = "/api/deployment-requests/1",
        andExpect = Ok
      ).contentString.parseJson.asJsObject.fields

      values1 should contain key "creationDate"
      val creationDate = values1("creationDate").toString.toLong
      creationDate should (be > 14e8.toLong and be < 20e8.toLong) // it's a timestamp in s.

      values1 shouldEqual Map(
        "id" -> JsNumber(1),
        "productName" -> JsString("my product"),
        "version" -> JsString("v21"),
        "target" -> JsString("to everywhere"),
        "reason" -> JsString("my comment"),
        "creator" -> JsString("anonymous"),
        "creationDate" -> JsNumber(creationDate)
      )
    }

    lazy val values3 = server.httpGet(
      path = "/api/deployment-requests/3",
      andExpect = Ok
    ).contentString.parseJson.asJsObject.fields

    "returns 200 and a JSON without `reason` if none or an empty one was provided" in {
      values3 should not contain key("reason")
    }

    "returns 200 and a JSON with the same target expression as provided" in {
      val target = values3("target")
      target shouldBe a[JsArray]
      target.asInstanceOf[JsArray].elements.map(
        el => {
          el shouldBe a[JsString]
          el.asInstanceOf[JsString].value
        }
      ) shouldEqual Vector("here", "and", "there")
    }

  }
}
