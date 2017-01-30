package com.criteo.perpetuo.app

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.{DbBinding, ProductBinder}
import com.criteo.perpetuo.model.Product
import com.twitter.finagle.http.Status.{BadRequest, Created, NotFound, Ok}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
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

    override protected def jacksonModule = CustomServerModules.jackson

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

  private def requestDeployment(productName: String, version: String, target: JsValue, comment: Option[String], expectsMessage: Option[String] = None) = {
    val ans = server.httpPost(
      path = "/api/deployment-requests",
      andExpect = if (expectsMessage.isDefined) BadRequest else Created,
      postBody = JsObject(
        Map(
          "productName" -> JsString(productName),
          "version" -> JsString(version),
          "target" -> target
        ) ++ (if (comment.isDefined) Map("comment" -> JsString(comment.get)) else Map())
      ).compactPrint
    ).contentString
    ans should include regex expectsMessage.getOrElse(""""id":\d+""")
    ans.parseJson.asJsObject
  }

  def insertProduct(name: String): String = {
    Await.result(controller.execution.dbBinding.insert(Product(None, name)), 1.second)
    name
  }

  "The DeploymentRequest's POST entry-point" should {

    "return 201 when creating a DeploymentRequest" in {
      requestDeployment(insertProduct("my product"), "v21", "to everywhere".toJson, Some("my comment"))
      requestDeployment(insertProduct("my other product"), "buggy", "nowhere".toJson, None)
    }

    "can handle a complex target expression" in {
      requestDeployment(insertProduct("my 3rd product"), "42", Seq("here", "and", "there").toJson, Some(""))
      requestDeployment(insertProduct("my 4th product"), "42", Map("select" -> "here").toJson, Some(""))
      requestDeployment(insertProduct("my 5th product"), "42", Map("select" -> Seq("here", "and", "there")).toJson, Some(""))
      requestDeployment(insertProduct("my 6th product"), "42", Seq(Map("select" -> Seq("here", "and", "there"))).toJson, Some(""))
    }

    "properly reject bad targets" in {
      val productName = insertProduct("a")
      requestDeployment(productName, "b", JsArray(), None, Some("non-empty JSON array or object"))
      requestDeployment(productName, "b", JsObject(), None, Some("must contain a field `select`"))
      requestDeployment(productName, "b", 60.toJson, None, Some("JSON array or object"))
      requestDeployment(productName, "b", Seq(42).toJson, None, Some("JSON object or string"))
      requestDeployment(productName, "b", Seq(JsObject()).toJson, None, Some("must contain a field `select`"))
      requestDeployment(productName, "b", Seq(Map("select" -> 42)).toJson, None, Some("non-empty JSON string or array"))
      requestDeployment(productName, "b", Seq(Map("select" -> Seq(42))).toJson, None, Some("a JSON string in"))
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
      val productName = insertProduct("some product")
      val o = requestDeployment(productName, "v2097", "to everywhere".toJson, Some("hello world"))
      val i = o.fields("id").asInstanceOf[JsNumber].value.toInt

      val values1 = server.httpGet(
        path = s"/api/deployment-requests/$i",
        andExpect = Ok
      ).contentString.parseJson.asJsObject.fields

      values1 should contain key "creationDate"
      val creationDate = values1("creationDate").toString.toLong
      creationDate should (be > 14e8.toLong and be < 20e8.toLong) // it's a timestamp in s.

      values1 shouldEqual Map(
        "id" -> JsNumber(i),
        "productName" -> JsString(productName),
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
}
