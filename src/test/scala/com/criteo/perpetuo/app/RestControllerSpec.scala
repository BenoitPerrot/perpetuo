package com.criteo.perpetuo.app

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.DeploymentRequestAttrs
import com.twitter.finagle.http.Status.{BadRequest, Conflict, Created, NotFound, Ok}
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.inject.server.FeatureTest
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsString, _}

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * An integration test for [[RestController]].
  */
class RestControllerSpec extends FeatureTest with TestDb {

  var controller: RestController = _

  val server = new EmbeddedHttpServer(new HttpServer {

    override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

    override def modules = Seq(
      dbTestModule
    )

    override def configureHttp(router: HttpRouter) {
      controller = injector.instance[RestController]
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .add(controller)
    }
  })

  private def product(name: String, expectedError: Option[(String, Status)] = None) = {
    val ans = server.httpPost(
      path = s"/api/products",
      andExpect = expectedError.map(_._2).getOrElse(Created),
      postBody = JsObject("name" -> JsString(name)).compactPrint
    ).contentString
    expectedError.foreach(err => ans shouldEqual JsObject("errors" -> JsArray(JsString(err._1))).compactPrint)
  }

  private def requestDeployment(productName: String, version: String, target: JsValue, comment: Option[String], expectsMessage: Option[String] = None): JsObject =
    requestDeployment(
      JsObject(
        Map(
          "productName" -> JsString(productName),
          "version" -> JsString(version),
          "target" -> target
        ) ++ (if (comment.isDefined) Map("comment" -> JsString(comment.get)) else Map())
      ).compactPrint,
      expectsMessage
    )

  private def requestDeployment(body: String, expectsMessage: Option[String]): JsObject = {
    val ans = server.httpPost(
      path = "/api/deployment-requests",
      andExpect = if (expectsMessage.isDefined) BadRequest else Created,
      postBody = body
    ).contentString
    ans should include regex expectsMessage.getOrElse(""""id":\d+""")
    ans.parseJson.asJsObject
  }

  private def checkCreationDate(depReqMap: Map[String, JsValue]): Long = {
    depReqMap should contain key "creationDate"
    val creationDate = depReqMap("creationDate").toString.toLong
    creationDate should (be > 14e8.toLong and be < 20e8.toLong) // it's a timestamp in s.
    creationDate
  }


  "The Product's entry-points" should {

    "return 201 when creating a Product" in {
      product("my product")
      product("my other product")
    }

    "properly reject already used names" in {
      product("my product", Some("Name `my product` is already used", Conflict))
    }

    "return the list of all known product names" in {
      val products = server.httpGet(
        path = "/api/products",
        andExpect = Ok
      ).contentString.parseJson.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsString].value)
      products should contain theSameElementsAs Seq("my product", "my other product")
    }

  }

  "The DeploymentRequest's POST entry-point" should {

    "return 201 when creating a DeploymentRequest" in {
      requestDeployment("my product", "v21", "to everywhere".toJson, Some("my comment"))
      requestDeployment("my other product", "buggy", "nowhere".toJson, None)
    }

    "properly reject bad input" in {
      requestDeployment("{", Some("Unexpected end-of-input at input"))
      requestDeployment("""{"productName": "abc"}""", Some("Expected to find `target`"))
      requestDeployment("""{"productName": "abc", "target": "*", "version": "2"}""", Some("Product `abc` could not be found"))
    }

    "handle a complex target expression" in {
      requestDeployment("my product", "42", Seq("here", "and", "there").toJson, Some(""))
      requestDeployment("my product", "42", Map("select" -> "here").toJson, Some(""))
      requestDeployment("my product", "42", Map("select" -> Seq("here", "and", "there")).toJson, Some(""))
      requestDeployment("my product", "42", Seq(Map("select" -> Seq("here", "and", "there"))).toJson, Some(""))
    }

    "properly reject bad targets" in {
      requestDeployment("my product", "b", JsArray(), None, Some("non-empty JSON array or object"))
      requestDeployment("my product", "b", JsObject(), None, Some("must contain a field `select`"))
      requestDeployment("my product", "b", 60.toJson, None, Some("JSON array or object"))
      requestDeployment("my product", "b", Seq(42).toJson, None, Some("JSON object or string"))
      requestDeployment("my product", "b", Seq(JsObject()).toJson, None, Some("must contain a field `select`"))
      requestDeployment("my product", "b", Seq(Map("select" -> 42)).toJson, None, Some("non-empty JSON string or array"))
      requestDeployment("my product", "b", Seq(Map("select" -> Seq(42))).toJson, None, Some("a JSON string in"))
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
      val o = requestDeployment("my product", "v2097", "to everywhere".toJson, Some("hello world"))
      val i = o.fields("id").asInstanceOf[JsNumber].value.toInt

      val values1 = server.httpGet(
        path = s"/api/deployment-requests/$i",
        andExpect = Ok
      ).contentString.parseJson.asJsObject.fields

      val creationDate = checkCreationDate(values1)

      values1 shouldEqual Map(
        "id" -> JsNumber(i),
        "productName" -> JsString("my product"),
        "version" -> JsString("v2097"),
        "target" -> JsString("to everywhere"),
        "comment" -> JsString("hello world"),
        "creator" -> JsString("anonymous"),
        "creationDate" -> JsNumber(creationDate)
      )
    }

    lazy val values3 = server.httpGet(
      path = "/api/deployment-requests/3",
      andExpect = Ok
    ).contentString.parseJson.asJsObject.fields

    "return 200 and a JSON without `comment` if none or an empty one was provided" in {
      values3 should not contain key("comment")
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

  "The ExecutionTrace's entry-points" should {

    "return 404 when trying to access a non-existing DeploymentRequest" in {
      server.httpGet(
        path = "/api/execution-traces/by-deployment-request/4242",
        andExpect = NotFound
      )
    }

    "not fail when the existing DeploymentRequest doesn't have execution traces yet" in {
      val attrs = new DeploymentRequestAttrs("my product", "v", "\"t\"", "c", "c", new Timestamp(System.currentTimeMillis))
      val depReq = Await.result(controller.execution.dbBinding.insert(attrs), 1.second)
      val traces = server.httpGet(
        path = s"/api/execution-traces/by-deployment-request/${depReq.id}",
        andExpect = Ok
      ).contentString.parseJson.asInstanceOf[JsArray].elements
      traces shouldBe empty
    }

    "return a list of executions when trying to access an existing DeploymentRequest" in {
      val traces = server.httpGet(
        path = "/api/execution-traces/by-deployment-request/1",
        andExpect = Ok
      ).contentString.parseJson.asInstanceOf[JsArray].elements
      traces.length shouldEqual 1
      traces.head.asJsObject.fields shouldEqual Map(
        "id" -> JsNumber(1),
        "logHref" -> JsNull,
        "state" -> JsString("pending")
      )
    }

  }

  "Any other *API* entry-point" should {
    "throw a 404" in {
      server.httpGet(
        path = "/api/woot/woot",
        andExpect = NotFound
      )
    }
  }

}
