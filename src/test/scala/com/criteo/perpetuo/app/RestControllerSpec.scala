package com.criteo.perpetuo.app

import com.twitter.finagle.http.Status.{BadRequest, Created, NotFound, Ok}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest
import com.typesafe.config.{Config, ConfigFactory}
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsString, _}


/**
  * An integration test for [[RestController]].
  */
class RestControllerSpec extends FeatureTest {

  val server = new EmbeddedHttpServer(new HttpServer {

    val config: Config = ConfigFactory.load()

    override protected def jacksonModule = CustomServerModules.jackson

    override def modules = Seq(
      new DbContextModule(config.getConfig("db").getConfig("test"))
    )

    override def configureHttp(router: HttpRouter) {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .add[RestController]
    }
  })

  private def requestDeployment(productName: String, version: String, target: JsValue, reason: Option[String], expectsMessage: Option[String] = None) = {
    val ans = server.httpPost(
      path = "/api/deployment-requests",
      andExpect = if (expectsMessage.isDefined) BadRequest else Created,
      postBody = JsObject(
        Map(
          "productName" -> JsString(productName),
          "version" -> JsString(version),
          "target" -> target
        ) ++ (if (reason.isDefined) Map("reason" -> JsString(reason.get)) else Map())
      ).compactPrint
    ).contentString
    ans should include regex expectsMessage.getOrElse(""""id":\d+""")
  }


  "The DeploymentRequest's POST entry-point" should {

    "return 201 when creating a DeploymentRequest" in {
      requestDeployment("my product", "v21", "to everywhere".toJson, Some("my comment"))
      requestDeployment("my other product", "buggy", "nowhere".toJson, None)
    }

    "can handle a complex target expression" in {
      requestDeployment("my 3rd product", "42", Seq("here", "and", "there").toJson, Some(""))
      requestDeployment("my 4th product", "42", Map("select" -> "here").toJson, Some(""))
      requestDeployment("my 5th product", "42", Map("select" -> Seq("here", "and", "there")).toJson, Some(""))
      requestDeployment("my 6th product", "42", Seq(Map("select" -> Seq("here", "and", "there"))).toJson, Some(""))
    }

    "properly reject bad targets" in {
      requestDeployment("a", "b", JsArray(), None, Some("non-empty JSON array or object"))
      requestDeployment("a", "b", JsObject(), None, Some("must contain a field `select`"))
      requestDeployment("a", "b", 60.toJson, None, Some("JSON array or object"))
      requestDeployment("a", "b", Seq(42).toJson, None, Some("JSON object or string"))
      requestDeployment("a", "b", Seq(JsObject()).toJson, None, Some("must contain a field `select`"))
      requestDeployment("a", "b", Seq(Map("select" -> 42)).toJson, None, Some("non-empty JSON string or array"))
      requestDeployment("a", "b", Seq(Map("select" -> Seq(42))).toJson, None, Some("a JSON string in"))
    }

  }


  "The DeploymentRequest's GET entry-point" should {

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

    "return 200 and a JSON with all necessary info when accessing an existing DeploymentRequest" in {
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

    "return 200 and a JSON without `reason` if none or an empty one was provided" in {
      values3 should not contain key("reason")
    }

    "return 200 and a JSON with the same target expression as provided" in {
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
