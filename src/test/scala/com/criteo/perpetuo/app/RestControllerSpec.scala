package com.criteo.perpetuo.app

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.DeploymentRequestAttrs
import com.twitter.finagle.http.Status._
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.inject.server.FeatureTest
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsString, _}

import scala.collection.mutable
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


  object T extends JsNumber(0) {
    override def equals(o: Any): Boolean = true
  }

  private val logHrefHistory: mutable.Map[Int, JsValue] = mutable.Map()

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
      path = "/api/deployment-requests?start=true",
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

  private def deepGetDepReq(): Seq[Map[String, JsValue]] = {
    server.httpGet(
      path = "/api/unstable/deployment-requests",
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields)
  }

  private def updateExecTrace(execId: Int, state: String,
                              logHref: Option[String], targetStatus: Option[Map[String, String]],
                              expectedTargetStatus: Map[String, String]) = {
    val logHrefJson = logHref.map(_.toJson)
    val previousLogHrefJson = logHrefHistory.getOrElse(execId, JsNull)
    val expectedLogHrefJson = logHrefJson.getOrElse(previousLogHrefJson)
    logHrefHistory(execId) = expectedLogHrefJson

    val params = Map(
      "state" -> Some(state.toJson),
      "logHref" -> logHrefJson,
      "targetStatus" -> targetStatus.map(_.toJson)
    ).collect {
      case (k, v) if v.isDefined => k -> v.get
    }
    val ret = server.httpPut(
      path = s"/api/execution-traces/$execId",
      putBody = params.toJson.compactPrint,
      andExpect = NoContent
    )
    ret.contentLength shouldEqual Some(0)

    val depReqs = deepGetDepReq()
    val getFirst: (JsValue) => Option[Map[String, JsValue]] =
      _.asInstanceOf[JsArray].elements.headOption.map(_.asJsObject.fields)
    val depReq = depReqs.find(req =>
      getFirst(req("operations")).map(_ ("executions")).flatMap(getFirst).exists(_ ("id") == execId.toJson)
    ).get
    val operations = depReq("operations").asInstanceOf[JsArray].elements.map(_.asJsObject.fields)
    operations.size shouldEqual 1
    Map(
      "id" -> T,
      "type" -> "deploy".toJson,
      "targetStatus" -> expectedTargetStatus.toJson,
      "executions" -> JsArray(
        JsObject(
          "id" -> execId.toJson,
          "logHref" -> expectedLogHrefJson,
          "state" -> state.toJson
        )
      )
    ) shouldEqual operations.head
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

      checkCreationDate(values1)

      Map(
        "id" -> JsNumber(i),
        "productName" -> JsString("my product"),
        "version" -> JsString("v2097"),
        "target" -> JsString("to everywhere"),
        "comment" -> JsString("hello world"),
        "creator" -> JsString("anonymous"),
        "creationDate" -> T
      ) shouldEqual values1
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
      Map(
        "id" -> T,
        "logHref" -> JsNull,
        "state" -> JsString("pending")
      ) shouldEqual traces.head.asJsObject.fields
    }

    "update one record's execution state on a PUT" in {
      updateExecTrace(
        1, "completed", None, None,
        expectedTargetStatus = Map()
      )
    }

    "update one record's execution state and log href on a PUT" in {
      updateExecTrace(
        2, "completed", Some("http://somewhe.re"), None,
        expectedTargetStatus = Map()
      )
    }

    "update one record's execution state and target status on a PUT" in {
      updateExecTrace(
        2, "initFailed", None, Some(Map("par" -> "success")),
        expectedTargetStatus = Map("par" -> "success")
      )
    }

    "update one record's execution state, log href and target status (partially) on a PUT" in {
      updateExecTrace(
        2, "conflicting", Some("http://"), Some(Map("am5" -> "notDone")),
        expectedTargetStatus = Map("par" -> "success", "am5" -> "notDone")
      )
    }

    "partially update one record's target status on a PUT" in {
      updateExecTrace(
        2, "completed", Some("http://final"), Some(Map("am5" -> "serverFailure")),
        expectedTargetStatus = Map("par" -> "success", "am5" -> "serverFailure")
      )
    }

    "return 404 on non-integral ID" in {
      server.httpPut(
        path = s"/api/execution-traces/abc",
        putBody = Map("state" -> "conflicting").toJson.compactPrint,
        andExpect = NotFound
      )
    }

    "return 404 if trying to update an unknown execution trace" in {
      server.httpPut(
        path = s"/api/execution-traces/12345",
        putBody = Map("state" -> "conflicting").toJson.compactPrint,
        andExpect = NotFound
      )
    }

    "return 400 if the target status is badly formatted and not update the state" in {
      server.httpPut(
        path = s"/api/execution-traces/1",
        putBody = JsObject("state" -> "conflicting".toJson, "targetStatus" -> Vector("par", "am5").toJson).compactPrint,
        andExpect = BadRequest
      ).contentString should include("targetStatus: Unable to parse")

      updateExecTrace(
        2, "completed", None, None,
        expectedTargetStatus = Map("par" -> "success", "am5" -> "serverFailure")
      )
    }

    "return 400 if no state is provided" in {
      server.httpPut(
        path = s"/api/execution-traces/1",
        putBody = JsObject("targetStatus" -> Map("par" -> "success").toJson).compactPrint,
        andExpect = BadRequest
      ).contentString should include("state: field is required")
    }

    "return 400 if the provided state is unknown" in {
      server.httpPut(
        path = s"/api/execution-traces/1",
        putBody = Map("state" -> "what?").toJson.compactPrint,
        andExpect = BadRequest
      ).contentString should include("Unknown state `what?`")
    }

    "return 400 if a provided target status is unknown and not update the state" in {
      server.httpPut(
        path = s"/api/execution-traces/1",
        putBody = JsObject("state" -> "conflicting".toJson, "targetStatus" -> Map("par" -> "foobar").toJson).compactPrint,
        andExpect = BadRequest
      ).contentString should include("Unknown target status `foobar`")

      updateExecTrace(
        2, "completed", None, None,
        expectedTargetStatus = Map("par" -> "success", "am5" -> "serverFailure")
      )
    }

  }

  "Deep query" should {
    "return the right executions in a valid JSON" in {
      val depReqs = deepGetDepReq()

      depReqs.length should be > 1
      val operationsCounts = depReqs.map(_ ("operations").asInstanceOf[JsArray].elements.size).toSet
      // there are deployment requests that triggered 1 operation, there is one with 0:
      operationsCounts shouldEqual Set(0, 1)

      val getFirst: (JsValue) => Option[Map[String, JsValue]] =
        _.asInstanceOf[JsArray].elements.headOption.map(_.asJsObject.fields)
      val depReq = depReqs.find(req =>
        getFirst(req("operations")).map(_ ("executions")).flatMap(getFirst).exists(_ ("id") == 2.toJson)
      ).get

      JsArray(
        JsObject(
          "id" -> T,
          "type" -> "deploy".toJson,
          "targetStatus" -> Map(
            "par" -> "success",
            "am5" -> "serverFailure"
          ).toJson,
          "executions" -> JsArray(
            JsObject(
              "id" -> 2.toJson,
              "logHref" -> "http://final".toJson,
              "state" -> "completed".toJson
            )
          )
        )
      ) shouldEqual depReq("operations")
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
