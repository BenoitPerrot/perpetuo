package com.criteo.perpetuo.app

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.auth.{User, UserFilter}
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Version}
import com.twitter.finagle.http.Status._
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.inject.server.FeatureTest
import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsObject, JsString, _}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * An integration test for [[RestController]].
  */
class RestControllerSpec extends FeatureTest with TestDb {

  val authModule = new AuthModule(AppConfig.under("auth"))
  val deployUser = User("qabot")
  val deployUserJWT = deployUser.toJWT(authModule.jwtEncoder)
  val stdUser = User("stdUser")
  val stdUserJWT = stdUser.toJWT(authModule.jwtEncoder)

  var controller: RestController = _

  val server = new EmbeddedHttpServer(new HttpServer {

    override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

    override def modules = Seq(
      authModule,
      dbTestModule
    )

    override def configureHttp(router: HttpRouter) {
      controller = injector.instance[RestController]
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .filter[UserFilter]
        .add(controller)
    }
  })


  object T extends JsNumber(0) {
    override def equals(o: Any): Boolean = true
  }

  private val logHrefHistory: mutable.Map[Int, JsValue] = mutable.Map()

  private def createProduct(name: String, expectedError: Option[(String, Status)] = None) = {
    val ans = server.httpPost(
      path = s"/api/products",
      headers = Map("Cookie" -> s"jwt=$deployUserJWT"),
      andExpect = expectedError.map(_._2).getOrElse(Created),
      postBody = JsObject("name" -> JsString(name)).compactPrint
    ).contentString
    expectedError.foreach(err => ans shouldEqual JsObject("errors" -> JsArray(JsString(err._1))).compactPrint)
  }

  private def requestDeployment(productName: String, version: String, target: JsValue, comment: Option[String], expectsMessage: Option[String] = None, start: Boolean = true): JsObject =
    requestDeployment(
      JsObject(
        Map(
          "productName" -> JsString(productName),
          "version" -> JsString(version),
          "target" -> target
        ) ++ (if (comment.isDefined) Map("comment" -> JsString(comment.get)) else Map())
      ).compactPrint,
      expectsMessage,
      start
    )

  private def requestDeployment(body: String, expectsMessage: Option[String], start: Boolean): JsObject = {
    val ans = server.httpPost(
      path = s"/api/deployment-requests?start=$start",
      headers = Map("Cookie" -> s"jwt=$deployUserJWT"),
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

  private def deepGetDepReq(where: Seq[Map[String, JsValue]] = Seq(), orderBy: Seq[Map[String, JsValue]] = Seq(), limit: Option[Int] = None, offset: Option[Int] = None): Seq[Map[String, JsValue]] = {
    val q = JsObject(
      Map(
        "where" -> where.toJson,
        "orderBy" -> orderBy.toJson
      )
        ++ limit.map { i => Map("limit" -> i.toJson) }.getOrElse(Map())
        ++ offset.map { i => Map("offset" -> i.toJson) }.getOrElse(Map())
    )
    server.httpPost(
      path = "/api/unstable/deployment-requests",
      postBody = q.compactPrint,
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsArray].elements.map(_.asJsObject.fields)
  }

  private def httpPut(path: String, body: JsValue, expect: Status) =
    server.httpPut(
      path = path,
      headers = Map("Cookie" -> s"jwt=$stdUserJWT"),
      putBody = body.compactPrint,
      andExpect = expect
    )

  private def updateExecTrace(execId: Int, state: String, logHref: Option[String],
                              targetStatus: Option[Map[String, JsValue]] = None,
                              expectedTargetStatus: Map[String, (String, String)]) = {
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
    val ret = httpPut(
      s"/api/execution-traces/$execId",
      params.toJson,
      NoContent
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
      "targetStatus" -> expectedTargetStatus.mapValues { case (s, d) => Map("code" -> s, "detail" -> d) }.toJson,
      "executions" -> JsArray(
        JsObject(
          "id" -> execId.toJson,
          "logHref" -> expectedLogHrefJson,
          "state" -> state.toJson
        )
      )
    ) shouldEqual operations.head
  }


  "Any protected route" should {
    "respond 401 if the user is not logged in" in {
      server.httpPost(
        path = s"/api/products",
        andExpect = Unauthorized,
        postBody = JsObject("name" -> JsString("this project will never be created")).compactPrint
      )
    }

    "respond 403 if the user is not allowed to do the operation" in {
      server.httpPost(
        path = s"/api/products",
        headers = Map("Cookie" -> s"jwt=$stdUserJWT"),
        andExpect = Forbidden,
        postBody = JsObject("name" -> JsString("this project will never be created")).compactPrint
      )
    }
  }

  "The Product's entry-points" should {

    "return 201 when creating a Product" in {
      createProduct("my product")
      createProduct("my other product")
    }

    "properly reject already used names" in {
      createProduct("my product", Some("Name `my product` is already used", Conflict))
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
      requestDeployment("{", Some("Unexpected end-of-input at input"), start = true)
      requestDeployment("""{"productName": "abc"}""", Some("Expected to find `target`"), start = true)
      requestDeployment("""{"productName": "abc", "target": "*", "version": "2"}""", Some("Product `abc` could not be found"), start = true)
    }

    "handle a complex target expression" in {
      requestDeployment("my product", "42", Seq("here", "and", "there").toJson, Some(""))
      requestDeployment("my product", "042", Map("select" -> "here").toJson, Some(""))
      requestDeployment("my product", "0042", Map("select" -> Seq("here", "and", "there")).toJson, Some(""))
      requestDeployment("my product", "420", Seq(Map("select" -> Seq("here", "and", "there"))).toJson, Some(""))
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

  "The DeploymentRequest's PUT entry-point" should {
    "start a deployment that was not started yet" in {
      val id = requestDeployment("my product", "not ready yet", "par".toJson, None, None, start = false)
        .fields("id").asInstanceOf[JsNumber].value
      httpPut(
        s"/api/deployment-requests/$id",
        "".toJson,
        Ok
      ).contentString.parseJson.asJsObject shouldEqual JsObject("id" -> id.toJson)
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
        "creator" -> JsString("qabot"),
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
      val attrs = new DeploymentRequestAttrs("my product", Version("v"), "\"t\"", "c", "c", new Timestamp(System.currentTimeMillis))
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
        1, "completed", None,
        expectedTargetStatus = Map()
      )
    }

    "update one record's execution state and log href on a PUT" in {
      updateExecTrace(
        2, "completed", Some("http://somewhe.re"),
        expectedTargetStatus = Map()
      )
    }

    "update one record's execution state and target status on a PUT" in {
      updateExecTrace(
        2, "initFailed", None,
        targetStatus = Some(Map("par" -> "success".toJson)),
        expectedTargetStatus = Map("par" -> ("success", ""))
      )
    }

    "update one record's execution state, log href and target status (partially) on a PUT" in {
      updateExecTrace(
        2, "conflicting", Some("http://"),
        targetStatus = Some(Map("am5" -> "notDone".toJson)),
        expectedTargetStatus = Map("par" -> ("success", ""), "am5" -> ("notDone", ""))
      )
    }

    "partially update one record's target status on a PUT" in {
      updateExecTrace(
        2, "completed", Some("http://final"),
        targetStatus = Some(Map("am5" -> Map("code" -> "hostFailure", "detail" -> "some details...").toJson)),
        expectedTargetStatus = Map("par" -> ("success", ""), "am5" -> ("hostFailure", "some details..."))
      )
    }

    "return 404 on non-integral ID" in {
      httpPut(
        s"/api/execution-traces/abc",
        Map("state" -> "conflicting").toJson,
        NotFound
      )
    }

    "return 404 if trying to update an unknown execution trace" in {
      httpPut(
        s"/api/execution-traces/12345",
        Map("state" -> "conflicting").toJson,
        NotFound
      )
    }

    "return 400 if the target status is badly formatted and not update the state" in {
      httpPut(
        s"/api/execution-traces/1",
        JsObject("state" -> "conflicting".toJson, "targetStatus" -> Vector("par", "am5").toJson),
        BadRequest
      ).contentString should include("targetStatus: Unable to parse")

      updateExecTrace(
        2, "completed", None,
        expectedTargetStatus = Map("par" -> ("success", ""), "am5" -> ("hostFailure", "some details..."))
      )
    }

    "return 400 if no state is provided" in {
      httpPut(
        s"/api/execution-traces/1",
        JsObject("targetStatus" -> Map("par" -> "success").toJson),
        BadRequest
      ).contentString should include("state: field is required")
    }

    "return 400 if the provided state is unknown" in {
      httpPut(
        s"/api/execution-traces/1",
        Map("state" -> "what?").toJson,
        BadRequest
      ).contentString should include("Unknown state `what?`")
    }

    "return 400 if a provided target status is unknown and not update the state" in {
      httpPut(
        s"/api/execution-traces/1",
        JsObject("state" -> "conflicting".toJson, "targetStatus" -> Map("par" -> "foobar").toJson),
        BadRequest
      ).contentString should include("Unknown target status `foobar`")

      updateExecTrace(
        2, "completed", None,
        targetStatus = Some(Map("am5" -> Map("code" -> "hostFailure", "detail" -> "some interesting details").toJson)),
        expectedTargetStatus = Map("par" -> ("success", ""), "am5" -> ("hostFailure", "some interesting details"))
      )
    }

  }

  "Deep query" should {
    "display correctly formatted versions" in {
      val depReqs = deepGetDepReq()
      depReqs.map(_ ("version").asInstanceOf[JsString].value) shouldEqual Vector(
        "v21", "buggy", "42", "42", "42", "420", "not ready yet", "v2097", "v"
      )
    }

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
            "par" -> Map("code" -> "success", "detail" -> "").toJson,
            "am5" -> Map("code" -> "hostFailure", "detail" -> "some interesting details").toJson
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

      val wasDelayed = depReqs.find(_ ("version") == "not ready yet".toJson).get
      JsArray(
        JsObject(
          "id" -> T,
          "type" -> "deploy".toJson,
          "targetStatus" -> JsObject(),
          "executions" -> JsArray(
            JsObject(
              "id" -> T,
              "logHref" -> JsNull,
              "state" -> "pending".toJson
            )
          )
        )
      ) shouldEqual wasDelayed("operations")
    }

    "paginate" in {
      val allDepReqs = deepGetDepReq()
      allDepReqs.length should be > 2

      val firstDepReqs = deepGetDepReq(limit = Some(2))
      firstDepReqs.length shouldEqual 2

      val lastDepReqs = deepGetDepReq(offset = Some(2))
      lastDepReqs.length shouldEqual (allDepReqs.length - 2)
    }

    "reject too large limits" in {
      server.httpPost(
        path = s"/api/unstable/deployment-requests",
        postBody = JsObject("limit" -> JsNumber(2097)).compactPrint,
        andExpect = BadRequest
      ).contentString should include("`limit` shall be lower than")
    }

    "filter" in {
      val allDepReqs = deepGetDepReq()
      allDepReqs.length should be > 2

      val depReqsForUnkownProduct = deepGetDepReq(where = Seq(Map("field" -> "productName".toJson, "equals" -> "unknown product".toJson)))
      depReqsForUnkownProduct.isEmpty shouldBe true

      val depReqsForSingleProduct = deepGetDepReq(where = Seq(Map("field" -> "productName".toJson, "equals" -> "my product".toJson)))
      depReqsForSingleProduct.length should(be > 0 and be < allDepReqs.length)
      depReqsForSingleProduct.map(_("productName").asInstanceOf[JsString].value == "my product").reduce(_ && _) shouldBe true
    }

    "reject unknown field names in filters" in {
      server.httpPost(
        path = s"/api/unstable/deployment-requests",
        postBody = JsObject("where" -> JsArray(Vector(JsObject("field" -> JsString("pouet"), "equals" -> JsString("42"))))).compactPrint,
        andExpect = BadRequest
      ).contentString should include("Cannot filter on `pouet`")
    }

    "reject unknown filter tests" in {
      server.httpPost(
        path = s"/api/unstable/deployment-requests",
        postBody = JsObject("where" -> JsArray(Vector(JsObject("field" -> JsString("productName"), "like" -> JsString("foo"))))).compactPrint,
        andExpect = BadRequest
      ).contentString should include("Filters tests must be `equals`")
    }

    "sort by individual fields" in {
      deepGetDepReq().length should be > 2

      def isSorted[ValueType, T <: { val value: ValueType }](deploymentRequests: Seq[Map[String, JsValue]], key: String, absoluteMin: ValueType, isOrdered: (ValueType, ValueType) => Boolean): Boolean = {
        deploymentRequests
          .foldLeft((absoluteMin, true)) { (lastResult, deploymentRequest) =>
            val value = deploymentRequest(key).asInstanceOf[T].value
            (value, isOrdered(lastResult._1, value))
          }
          ._2
      }

      Seq(false, true).foreach { descending =>

        val sortNumbers = isSorted[BigDecimal, JsNumber](_: Seq[Map[String, JsValue]], _: String, BigDecimal.valueOf(-1), _ <= _)
        val sortStrings = isSorted[String, JsString](_: Seq[Map[String, JsValue]], _: String, "", _ <= _)
        val sortVersions = isSorted[String, JsString](_: Seq[Map[String, JsValue]], _: String, "", Version(_).value <= Version(_).value)
        Map(
          "creationDate" -> sortNumbers,
          "productName" -> sortStrings,
          "creator" -> sortStrings,
          "version" -> sortVersions
        ).foreach { case (fieldName, isSorted) =>
            val sortedDepReqs = deepGetDepReq(orderBy = Seq(Map("field" -> fieldName.toJson, "desc" -> descending.toJson)))
            sortedDepReqs.isEmpty shouldBe false
            isSorted(if (descending) sortedDepReqs.reverse else sortedDepReqs, fieldName) shouldBe true
        }
      }
    }

    "sort by several fields" in {
      deepGetDepReq().length should be > 2

      val sortedDepReqs = deepGetDepReq(orderBy = Seq(Map("field" -> "productName".toJson), Map("field" -> "creationDate".toJson)))
      sortedDepReqs.isEmpty shouldBe false
      sortedDepReqs
        .foldLeft(("", BigDecimal(-1), true)) { (lastResult, deploymentRequest) =>
          val (lastProductName, lastCreationDate, lastP) = lastResult
          val productName = deploymentRequest("productName").asInstanceOf[JsString].value
          val creationDate = deploymentRequest("creationDate").asInstanceOf[JsNumber].value
          // For the right branch of the ||: productName == lastProductName is paranoid, as productName < lastProductName should not be true
          (productName, creationDate, lastP && (lastProductName < productName || (productName == lastProductName && lastCreationDate <= creationDate)))
        }
        ._3 shouldBe true
    }

    "reject unknown field names for sort" in {
      server.httpPost(
        path = s"/api/unstable/deployment-requests",
        postBody = JsObject("orderBy" -> JsArray(JsObject("field" -> "pouet".toJson))).compactPrint,
        andExpect = BadRequest
      ).contentString should include("Cannot sort by `pouet`")
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
