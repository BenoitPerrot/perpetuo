package com.criteo.perpetuo.app

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.auth.{User, UserFilter}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, ProtoDeploymentPlanStep, Version}
import com.twitter.finagle.http.Status._
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finatra.http.filters.{LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{EmbeddedHttpServer, HttpServer}
import com.twitter.finatra.json.modules.FinatraJacksonModule
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * An integration test for [[RestController]].
  */
class RestControllerSpec extends Test with TestDb {

  implicit class JsObjectIdExtractor(private val o: JsValue) {
    def idAsLong: Long = o.asJsObject.fields("id").asInstanceOf[JsNumber].value.longValue
  }

  private def makeUser(userName: String) = User(userName, Set("Users"))

  val config = AppConfigProvider.config
  val authModule = new AuthModule(config.getConfig("auth"))
  val productUser = makeUser("bob.the.producer")
  val productUserJWT = productUser.toJWT(authModule.jwtEncoder)
  val deployUser = makeUser("r.eleaser")
  val deployUserJWT = deployUser.toJWT(authModule.jwtEncoder)
  val stdUser = makeUser("stdUser")
  val stdUserJWT = stdUser.toJWT(authModule.jwtEncoder)

  var controller: RestController = _

  val server = new EmbeddedHttpServer(new HttpServer {

    override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

    override def modules = Seq(
      authModule,
      new PluginsModule,
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
    override def toString(): String = "?"

    override def equals(o: Any): Boolean = true
  }

  private val logHrefHistory: mutable.Map[Long, JsValue] = mutable.Map()

  private def createProduct(name: String, expectedError: Option[(String, Status)] = None): Unit = {
    val ans = server.httpPost(
      path = s"/api/products",
      headers = Map("Cookie" -> s"jwt=$productUserJWT"),
      andExpect = expectedError.map(_._2).getOrElse(Created),
      postBody = JsObject("name" -> JsString(name)).compactPrint
    ).contentString
    expectedError.foreach(err => ans shouldEqual JsObject("errors" -> JsArray(JsString(err._1))).compactPrint)
  }

  private def requestAndWaitDeployment(productName: String, version: String, target: JsValue, comment: Option[String] = None, expectsMessage: Option[String] = None): Long = {
    val depReqId = requestDeployment(productName, version, target, comment, expectsMessage)
    expectsMessage.getOrElse {
      startDeploymentRequest(depReqId)
      getExecutionTracesByDeploymentRequestId(depReqId.toString).elements.map(_.idAsLong).foreach(execTraceId =>
        checkExecutionTraceUpdate(depReqId, execTraceId, "completed")
      )
    }
    depReqId
  }

  private def requestDeployment(productName: String, version: String, target: JsValue, comment: Option[String] = None, expectsMessage: Option[String] = None): Long = {
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
  }

  private def requestDeployment(body: String, expectsMessage: Option[String]): Long = {
    val ans = server.httpPost(
      path = s"/api/deployment-requests",
      headers = Map("Cookie" -> s"jwt=$deployUserJWT"),
      andExpect = if (expectsMessage.isDefined) BadRequest else Created,
      postBody = body
    ).contentString
    ans should include regex expectsMessage.getOrElse(""""id":\d+""")
    expectsMessage.map(_ => -1L).getOrElse(ans.parseJson.asJsObject.fields("id").asInstanceOf[JsNumber].value.toLongExact)
  }

  private def checkCreationDate(depReqMap: Map[String, JsValue]): Long = {
    depReqMap should contain key "creationDate"
    val creationDate = depReqMap("creationDate").toString.toLong
    creationDate should (be > 14e8.toLong and be < 20e8.toLong) // it's a timestamp in s.
    creationDate
  }

  private def deepGetDepReq(id: Long) = {
    server.httpGet(
      path = s"/api/unstable/deployment-requests/$id",
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsObject]
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

  private def putExecutionTrace(execTraceId: String, body: JsValue, expect: Status) =
    server.httpPut(
      path = RestApi.executionCallbackPath(execTraceId),
      putBody = body.compactPrint,
      andExpect = expect
    )

  private def actOnDeploymentRequest(deploymentRequestId: Long, operationName: String, body: JsValue, expect: Status): Response =
    server.httpPost(
      path = s"/api/deployment-requests/$deploymentRequestId/actions/$operationName",
      headers = Map("Cookie" -> s"jwt=$deployUserJWT"),
      postBody = body.compactPrint,
      andExpect = expect
    )

  private def startDeploymentRequest(deploymentRequestId: Long, expect: Status): Response =
    actOnDeploymentRequest(deploymentRequestId, "deploy", JsObject(), expect)

  private def startDeploymentRequest(deploymentRequestId: Long): JsObject =
    startDeploymentRequest(deploymentRequestId, Ok).contentString.parseJson.asJsObject

  private val randomProductCounter = Stream.from(1000).iterator

  private def createProductAndStartDeployment(version: String, target: JsValue) = {
    val productName = s"random product ${randomProductCounter.next()}"
    createProduct(productName)
    val depReqId = requestAndWaitDeployment(productName, version, target)
    (depReqId, getExecutionTracesByDeploymentRequestId(depReqId.toString).elements.map(_.idAsLong))
  }

  private def updateExecutionTrace(execTraceId: Long, state: String,
                                   logHref: Option[String] = None,
                                   targetStatus: Option[Map[String, JsValue]] = None,
                                   executionDetail: Option[String] = None,
                                   expectedRequestStatus: Status = NoContent): Unit = {
    val params = Map(
      "state" -> Some(state.toJson),
      "logHref" -> logHref.map(_.toJson),
      "detail" -> executionDetail.map(_.toJson),
      "targetStatus" -> targetStatus.map(_.toJson)
    ).collect {
      case (k, v) if v.isDefined => k -> v.get
    }
    putExecutionTrace(
      execTraceId.toString,
      params.toJson,
      expectedRequestStatus
    )
  }

  private def checkExecutionTraceUpdate(deploymentRequestId: Long, execTraceId: Long, state: String,
                                        logHref: Option[String] = None,
                                        targetStatus: Option[Map[String, JsValue]] = None,
                                        executionDetail: Option[String] = None,
                                        expectedTargetStatus: Map[String, (String, String)] = Map(),
                                        expectedRequestStatus: Status = NoContent,
                                        expectedClosed: Boolean = true): Unit = {
    val previousLogHrefJson = logHrefHistory.getOrElse(execTraceId, JsNull)
    val expectedLogHrefJson = logHref.map(_.toJson).getOrElse(previousLogHrefJson)
    logHrefHistory(execTraceId) = expectedLogHrefJson

    updateExecutionTrace(execTraceId, state, logHref, targetStatus, executionDetail, expectedRequestStatus)

    val depReq = deepGetDepReq(deploymentRequestId)
    val operations = depReq.fields("operations").asInstanceOf[JsArray].elements.map(_.asJsObject.fields)
    operations.size shouldEqual 1
    Map(
      "id" -> T,
      "kind" -> "deploy".toJson,
      "creator" -> "r.eleaser".toJson,
      "creationDate" -> T,
      "targetStatus" -> expectedTargetStatus.mapValues { case (s, d) => Map("code" -> s, "detail" -> d) }.toJson,
      "executions" -> JsArray(
        JsObject(
          "id" -> execTraceId.toJson,
          "logHref" -> expectedLogHrefJson,
          "state" -> state.toJson,
          "detail" -> executionDetail.getOrElse("").toJson
        )
      )
    ) ++ (if (expectedClosed) Map("closingDate" -> T) else Map()) shouldEqual operations.head
  }

  test("The Product's entry-point returns 201 when creating a Product") {
    createProduct("my product")
    createProduct("my other product")
  }

  test("The Product's entry-point properly rejects already used names") {
    createProduct("my product", Some("Name `my product` is already used", Conflict))
  }

  test("The Product's entry-point returns the list of all known product names") {
    val products = server.httpGet(
      path = "/api/products",
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsString].value)
    products should contain theSameElementsAs Seq("my product", "my other product")
  }

  test("The DeploymentRequest's POST entry-point returns 201 when creating a DeploymentRequest") {
    requestAndWaitDeployment("my product", "v21", "to everywhere".toJson, Some("my comment"))
    requestAndWaitDeployment("my other product", "buggy", "nowhere".toJson, None)
  }

  test("The DeploymentRequest's POST entry-point properly rejects bad input") {
    requestDeployment("{", Some("Unexpected end-of-input at input"))
    requestDeployment("""{"productName": "abc"}""", Some("Expected to find `version`"))
    requestDeployment("""{"productName": "abc", "version": "42"}""", Some("Expected to find `target`"))
    requestDeployment("""{"productName": "abc", "target": "*", "version": "2"}""", Some("Product `abc` could not be found"))
  }

  test("The DeploymentRequest's POST entry-point handles a complex target expression") {
    requestAndWaitDeployment("my product", "42", Seq("here", "and", "there").toJson, Some(""))
    requestAndWaitDeployment("my product", " 10402", Map("select" -> "here").toJson, Some(""))
    requestAndWaitDeployment("my product", "0042", Map("select" -> Seq("here", "and", "there")).toJson, Some(""))
    requestAndWaitDeployment("my product", "420", Seq(Map("select" -> Seq("here", "and", "there"))).toJson, Some(""))
  }

  test("The DeploymentRequest's POST entry-point properly rejects bad targets") {
    requestAndWaitDeployment("my product", "b", JsArray(), None, Some("non-empty JSON array or object"))
    requestAndWaitDeployment("my product", "b", JsObject(), None, Some("must contain a field `select`"))
    requestAndWaitDeployment("my product", "b", 60.toJson, None, Some("JSON array or object"))
    requestAndWaitDeployment("my product", "b", Seq(42).toJson, None, Some("JSON object or string"))
    requestAndWaitDeployment("my product", "b", Seq(JsObject()).toJson, None, Some("must contain a field `select`"))
    requestAndWaitDeployment("my product", "b", Seq(Map("select" -> 42)).toJson, None, Some("non-empty JSON string or array"))
    requestAndWaitDeployment("my product", "b", Seq(Map("select" -> Seq(42))).toJson, None, Some("a JSON string in"))
  }

  test("The DeploymentRequest's actions entry-point starts a deployment that was not started yet") {
    createProduct("my product B")
    val id = requestDeployment("my product B", "456", "ams".toJson)
    startDeploymentRequest(id) shouldEqual JsObject("id" -> id.toJson)
  }

  test("The DeploymentRequest's actions entry-point returns 404 when trying to start a non-existing DeploymentRequest") {
    startDeploymentRequest(4242, NotFound)
  }

  test("The DeploymentRequest's actions entry-point returns 404 when posting a non-existing action") {
    actOnDeploymentRequest(1, "ploup", JsObject(), NotFound)
  }

  test("The DeploymentRequest's actions entry-point returns 422 when trying to revert new targets") {
    val getRespJson = (_: Response).contentString.parseJson.asJsObject.fields.map {
      case ("errors", v: JsArray) => ("error", v.elements.head.asInstanceOf[JsString].value)
      case (k, v) => (k, v.asInstanceOf[JsString].value)
    }

    createProduct("my new product")
    val id = requestDeployment("my new product", "789", "par".toJson)
    val respJson1 = getRespJson(actOnDeploymentRequest(id, "revert", JsObject(), UnprocessableEntity))
    respJson1("error") should include("it has not yet been applied")
    respJson1 shouldNot contain("required")

    startDeploymentRequest(id, Ok)
    val execTraceId = getExecutionTracesByDeploymentRequestId(id.toString, Ok).get.elements.head.idAsLong
    checkExecutionTraceUpdate(
      id, execTraceId, "completed", None, Some(
        Map("targetA" -> Map("code" -> "success", "detail" -> "").toJson,
          "targetB" -> Map("code" -> "productFailure", "detail" -> "").toJson)),
      None, Map("targetA" -> ("success", ""), "targetB" -> ("productFailure", ""))
    )
    val respJson2 = getRespJson(actOnDeploymentRequest(id, "revert", JsObject(), UnprocessableEntity))
    respJson2("error") should include("a default rollback version is required")
    respJson2("required") shouldEqual "defaultVersion"

    actOnDeploymentRequest(id, "revert", Map("defaultVersion" -> "42").toJson, Ok)
  }

  private def getDeploymentRequest(id: String, expectedStatus: Status): Option[JsObject] = {
    val response = server.httpGet(
      path = s"/api/deployment-requests/$id",
      andExpect = expectedStatus
    )
    if (response.status == Ok)
      Some(response.contentString.parseJson.asJsObject)
    else
      None
  }

  private def getDeploymentRequest(id: String): JsObject =
    getDeploymentRequest(id, Ok).get

  private def getExecutionTracesByDeploymentRequestId(deploymentRequestId: String, expectedStatus: Status): Option[JsArray] = {
    val response = server.httpGet(
      path = s"/api/deployment-requests/$deploymentRequestId/execution-traces",
      andExpect = expectedStatus
    )
    if (response.status == Ok)
      Some(response.contentString.parseJson.asInstanceOf[JsArray])
    else
      None
  }

  private def getExecutionTracesByDeploymentRequestId(deploymentRequestId: String): JsArray =
    getExecutionTracesByDeploymentRequestId(deploymentRequestId, Ok).get

  test("The DeploymentRequest's GET entry-point returns 404 when trying to access a non-existing DeploymentRequest") {
    getDeploymentRequest("4242", NotFound)
  }

  test("The DeploymentRequest's GET entry-point returns 404 when trying to access a DeploymentRequest with a non-integral ID") {
    getDeploymentRequest("..", NotFound)
  }

  test("The DeploymentRequest's GET entry-point returns 200 and a JSON with all necessary info when accessing an existing DeploymentRequest") {
    val depReqId = requestAndWaitDeployment("my product", "v2097", "to everywhere".toJson, Some("hello world"))

    val values1 = getDeploymentRequest(depReqId.toString).fields

    checkCreationDate(values1)

    Map(
      "id" -> JsNumber(depReqId),
      "productName" -> JsString("my product"),
      "version" -> JsString("v2097"),
      "target" -> JsString("to everywhere"),
      "comment" -> JsString("hello world"),
      "creator" -> JsString("r.eleaser"),
      "creationDate" -> T
    ) shouldEqual values1
  }

  lazy val values3 = getDeploymentRequest("3").fields

  test("The DeploymentRequest's GET entry-point returns 200 and a JSON without `comment` if none or an empty one was provided") {
    values3 should not contain key("comment")
  }

  test("The DeploymentRequest's GET entry-point returns 200 and a JSON with the same target expression as provided") {
    val target = values3("target")
    target shouldBe a[JsArray]
    target.asInstanceOf[JsArray].elements.map(
      el => {
        el shouldBe a[JsString]
        el.asInstanceOf[JsString].value
      }
    ) shouldEqual Vector("here", "and", "there")
  }

  test("The ExecutionTrace's entry-point returns 404 when trying to access a non-existing DeploymentRequest") {
    getExecutionTracesByDeploymentRequestId("4242", NotFound)
  }

  test("The ExecutionTrace's entry-point doesn't fail when the existing DeploymentRequest doesn't have execution traces yet") {
    val attrs = new DeploymentRequestAttrs("my product", Version("\"v\""), Seq(ProtoDeploymentPlanStep("", JsString("t"), "")), "c", "c")
    val depReq = Await.result(controller.engine.crankshaft.createDeploymentRequest(attrs), 1.second)
    val traces = getExecutionTracesByDeploymentRequestId(depReq.toString).elements
    traces shouldBe empty
  }

  test("The ExecutionTrace's entry-point returns a list of executions when trying to access a completed DeploymentRequest") {
    val traces = getExecutionTracesByDeploymentRequestId("1").elements
    traces.length shouldEqual 1
    Map(
      "id" -> T,
      "logHref" -> JsNull,
      "state" -> "completed".toJson,
      "detail" -> "".toJson
    ) shouldEqual traces.head.asJsObject.fields
  }

  test("The ExecutionTrace's entry-point updates one record's execution state on a PUT") {
    val (depReqId, executionTraces) = createProductAndStartDeployment("1112", "paris".toJson)
    checkExecutionTraceUpdate(depReqId, executionTraces.head, "completed", executionDetail = Some("execution detail"))
  }

  test("The ExecutionTrace's entry-point updates one record's execution state and log href on a PUT") {
    val (depReqId, executionTraces) = createProductAndStartDeployment("1112", "paris".toJson)
    checkExecutionTraceUpdate(depReqId, executionTraces.head, "completed", Some("http://somewhe.re"))
  }

  test("The ExecutionTrace's entry-point updates one record's execution state and target status on a PUT") {
    val productName = s"random product ${randomProductCounter.next()}"
    createProduct(productName)
    val depReqId = requestDeployment(productName, "653", Seq("paris", "ams").toJson)
    startDeploymentRequest(depReqId)
    getExecutionTracesByDeploymentRequestId(depReqId.toString).elements.map(_.idAsLong).foreach(
      checkExecutionTraceUpdate(
        depReqId, _, "initFailed", None,
        Some(Map("paris" -> Map("code" -> "success", "detail" -> "").toJson)),
        None, Map("paris" -> ("success", ""))
      )
    )
  }

  test("The ExecutionTrace's entry-point updates one record's execution state, log href and target status (partially) on a PUT") {
    val depReqId = requestDeployment("my product", "653", Seq("paris", "amsterdam").toJson, None)
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString).elements(0).idAsLong
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "conflicting", Some("http://"),
      Some(Map("amsterdam" -> Map("code" -> "notDone", "detail" -> "").toJson)),
      None, Map("amsterdam" -> ("notDone", ""))
    )
  }

  test("The ExecutionTrace's entry-point partially updates one record's target status on a PUT") {
    val depReqId = requestDeployment("my product", "653", Seq("paris", "amsterdam").toJson, None)
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString).elements(0).idAsLong
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "running", None,
      Some(Map("amsterdam" -> Map("code" -> "notDone", "detail" -> "").toJson)),
      None, Map("amsterdam" -> ("notDone", "")),
      expectedClosed = false
    )
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "running", None,
      Some(Map("amsterdam" -> Map("code" -> "running", "detail" -> "").toJson, "paris" -> Map("code" -> "notDone", "detail" -> "waiting...").toJson)),
      None, Map("amsterdam" -> ("running", ""), "paris" -> ("notDone", "waiting...")),
      expectedClosed = false
    )
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "stopped", Some("http://final"),
      Some(Map(
        "tokyo" -> Map("code" -> "notDone", "detail" -> "crashed").toJson,
        "london" -> Map("code" -> "running", "detail" -> "am I too late?").toJson
      )),
      None,
      Map(
        "paris" -> ("notDone", "waiting..."), // untouched; example of a case where the detail doesn't make sense anymore (while it still might)
        "amsterdam" -> ("undetermined", "No feedback from the executor"), // auto-updated on close
        "tokyo" -> ("notDone", "crashed"), // "manually" updated on close
        "london" -> ("undetermined", "No feedback from the executor") // "manually" but then automatically updated on close
      )
    )
  }

  test("The ExecutionTrace's entry-point rejects impossible transitions") {
    createProduct("calm-camel")
    val depReqId = requestDeployment("calm-camel", "123456", Seq("paris", "amsterdam").toJson, None)
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString).elements(0).idAsLong
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "conflicting", None,
      Some(Map("amsterdam" -> Map("code" -> "notDone", "detail" -> "").toJson)),
      None, Map("amsterdam" -> ("notDone", ""))
    )
    updateExecutionTrace(
      execTraceId, "completed", Some("http://final"),
      Some(Map("paris" -> Map("code" -> "hostFailure", "detail" -> "crashed").toJson)), None,
      UnprocessableEntity
    )
  }

  test("The ExecutionTrace's entry-point returns 404 on non-integral ID") {
    putExecutionTrace(
      "abc",
      Map("state" -> "conflicting").toJson,
      NotFound
    )
  }

  test("The ExecutionTrace's entry-point returns 404 if trying to update an unknown execution trace") {
    putExecutionTrace(
      "12345",
      Map("state" -> "conflicting").toJson,
      NotFound
    )
  }

  test("The ExecutionTrace's entry-point returns 400 if the target status is badly formatted and not update the state") {
    val (depReqId, executionTraces) = createProductAndStartDeployment("2456", Seq("paris", "amsterdam").toJson)
    val execTraceId = executionTraces.head

    putExecutionTrace(
      execTraceId.toString,
      JsObject("state" -> "conflicting".toJson, "targetStatus" -> Vector("paris", "amsterdam").toJson),
      BadRequest
    ).contentString should include("targetStatus: Can not deserialize")

    checkExecutionTraceUpdate(depReqId, execTraceId, "completed")
  }

  test("The ExecutionTrace's entry-point returns 400 if no state is provided") {
    putExecutionTrace(
      "1",
      JsObject("targetStatus" -> Map("par" -> "success").toJson),
      BadRequest
    ).contentString should include("state: field is required")
  }

  test("The ExecutionTrace's entry-point returns 400 if the provided state is unknown") {
    val (_, executionTraces) = createProductAndStartDeployment("2456", Seq("paris", "amsterdam").toJson)

    putExecutionTrace(
      executionTraces.head.toString,
      Map("state" -> "what?").toJson,
      BadRequest
    ).contentString should include("Unknown state `what?`")
  }

  test("The ExecutionTrace's entry-point returns 400 and not update the state if a provided target status is unknown") {
    val (depReqId, executionTraces) = createProductAndStartDeployment("2456", Seq("paris", "amsterdam").toJson)
    val execTraceId = executionTraces.head

    putExecutionTrace(
      execTraceId.toString,
      JsObject("state" -> "conflicting".toJson, "targetStatus" -> Map("par" -> Map("code" -> "foobar", "detail" -> "")).toJson),
      BadRequest
    ).contentString should include("Bad target status for `par`: code='foobar', detail=''")

    checkExecutionTraceUpdate(depReqId, execTraceId, "completed")
  }

  test("The stop entry-point tries to stop running executions for a given deployment request") {
    val productName = s"product-${randomProductCounter.next()}"
    createProduct(productName)
    val depReqId = requestDeployment(productName, "653", Seq("paris", "ams").toJson)
    startDeploymentRequest(depReqId)

    actOnDeploymentRequest(depReqId, "stop", JsObject(), Status.Ok).contentString.parseJson.asJsObject shouldEqual
      JsObject(
        "id" -> JsNumber(depReqId),
        "stopped" -> JsNumber(0),
        "failures" -> JsArray(JsString(s"No log href for execution trace #$depReqId, thus cannot interact with the actual execution"))
      )

    getExecutionTracesByDeploymentRequestId(depReqId.toString).elements.map(_.idAsLong).foreach { execTraceId =>
      updateExecutionTrace(
        execTraceId, "completed", Some("http://final"),
        Some(Map("paris" -> Map("code" -> "hostFailure", "detail" -> "crashed").toJson))
      )
    }
    actOnDeploymentRequest(depReqId, "stop", JsObject(), Status.Ok).contentString.parseJson.asJsObject shouldEqual
      JsObject("id" -> JsNumber(depReqId), "stopped" -> JsNumber(0), "failures" -> JsArray())
  }

  test("Deep query selects single one") {
    val allDepReqs = deepGetDepReq()
    allDepReqs.length should be > 2

    val depReqsForSingle = deepGetDepReq(where = Seq(Map("field" -> JsString("id"), "equals" -> JsNumber(1))))
    depReqsForSingle.length shouldBe 1
  }

  test("Deep query gets single one") {
    val allDepReqs = deepGetDepReq()
    allDepReqs.length should be > 2

    val depReqsForSingle = deepGetDepReq(1)
    depReqsForSingle.idAsLong shouldBe 1
  }

  test("Deep query displays correctly formatted versions") {
    val depReqId = requestDeployment("my product", "8080", "paris".toJson)

    val depReq = deepGetDepReq(depReqId)
    depReq.fields("version").asInstanceOf[JsString].value shouldEqual "8080"
  }

  test("Deep query returns the right executions in a valid JSON") {
    val (depReqId, executionTraces) = createProductAndStartDeployment("3211", Seq("paris", "amsterdam").toJson)
    val execTraceId = executionTraces.head
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "completed", Some("http://final"),
      Some(Map(
        "paris" -> Map("code" -> "success", "detail" -> "").toJson,
        "amsterdam" -> Map("code" -> "hostFailure", "detail" -> "some interesting details").toJson)),
      None, Map(
        "paris" -> ("success", ""),
        "amsterdam" -> ("hostFailure", "some interesting details"))
    )

    val depReq = deepGetDepReq(depReqId)

    JsArray(
      JsObject(
        "id" -> T,
        "kind" -> "deploy".toJson,
        "creator" -> "r.eleaser".toJson,
        "creationDate" -> T,
        "closingDate" -> T,
        "targetStatus" -> Map(
          "paris" -> Map("code" -> "success", "detail" -> "").toJson,
          "amsterdam" -> Map("code" -> "hostFailure", "detail" -> "some interesting details").toJson
        ).toJson,
        "executions" -> JsArray(
          JsObject(
            "id" -> execTraceId.toJson,
            "logHref" -> "http://final".toJson,
            "state" -> "completed".toJson,
            "detail" -> "".toJson
          )
        )
      )
    ) shouldEqual depReq.fields("operations")

    createProduct("my product D")
    val delayedDepReqId = requestDeployment("my product D", "5133", "tokyo".toJson)
    startDeploymentRequest(delayedDepReqId)
    val delayedDepReq = deepGetDepReq(delayedDepReqId)
    JsArray(
      JsObject(
        "id" -> T,
        "kind" -> "deploy".toJson,
        "creator" -> "r.eleaser".toJson,
        "creationDate" -> T,
        "targetStatus" -> JsObject(),
        "executions" -> JsArray(
          JsObject(
            "id" -> T,
            "logHref" -> JsNull,
            "state" -> "pending".toJson,
            "detail" -> "".toJson
          )
        )
      )
    ) shouldEqual delayedDepReq.fields("operations")
  }

  test("Deep query paginates") {
    val allDepReqs = deepGetDepReq(limit = Some(100))
    allDepReqs.length should be > 2

    val firstDepReqs = deepGetDepReq(limit = Some(2))
    firstDepReqs.length shouldEqual 2

    val lastDepReqs = deepGetDepReq(offset = Some(2), limit = Some(100))
    lastDepReqs.length shouldEqual (allDepReqs.length - 2)
  }

  test("Deep query rejects too large limits") {
    server.httpPost(
      path = s"/api/unstable/deployment-requests",
      postBody = JsObject("limit" -> JsNumber(2097)).compactPrint,
      andExpect = BadRequest
    ).contentString should include("`limit` shall be lower than")
  }

  test("Deep query filters") {
    val allDepReqs = deepGetDepReq(limit = Some(100))
    allDepReqs.length should be > 2

    val depReqsForUnkownProduct = deepGetDepReq(where = Seq(Map("field" -> "productName".toJson, "equals" -> "unknown product".toJson)))
    depReqsForUnkownProduct.isEmpty shouldBe true

    val depReqsForSingleProduct = deepGetDepReq(where = Seq(Map("field" -> "productName".toJson, "equals" -> "my product".toJson)))
    depReqsForSingleProduct.length should (be > 0 and be < allDepReqs.length)
    depReqsForSingleProduct.map(_ ("productName").asInstanceOf[JsString].value == "my product").reduce(_ && _) shouldBe true
  }

  test("Deep query rejects unknown field names in filters") {
    server.httpPost(
      path = s"/api/unstable/deployment-requests",
      postBody = JsObject("where" -> JsArray(Vector(JsObject("field" -> JsString("pouet"), "equals" -> JsString("42"))))).compactPrint,
      andExpect = BadRequest
    ).contentString should include("Cannot filter on `pouet`")
  }

  test("Deep query rejects unknown filter tests") {
    server.httpPost(
      path = s"/api/unstable/deployment-requests",
      postBody = JsObject("where" -> JsArray(Vector(JsObject("field" -> JsString("productName"), "like" -> JsString("foo"))))).compactPrint,
      andExpect = BadRequest
    ).contentString should include("Filters tests must be `equals`")
  }

  test("Deep query sorts by individual fields") {
    deepGetDepReq().length should be > 2

    def isSorted[ValueType, T <: {val value : ValueType}](deploymentRequests: Seq[Map[String, JsValue]], key: String, absoluteMin: ValueType, isOrdered: (ValueType, ValueType) => Boolean): Boolean = {
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
      Map(
        "creationDate" -> sortNumbers,
        "productName" -> sortStrings,
        "creator" -> sortStrings
      ).foreach { case (fieldName, isSorted) =>
        val sortedDepReqs = deepGetDepReq(orderBy = Seq(Map("field" -> fieldName.toJson, "desc" -> descending.toJson)))
        sortedDepReqs.isEmpty shouldBe false
        isSorted(if (descending) sortedDepReqs.reverse else sortedDepReqs, fieldName) shouldBe true
      }
    }
  }

  test("Deep query sorts by several fields") {
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

  test("Deep query rejects unknown field names for sort") {
    server.httpPost(
      path = s"/api/unstable/deployment-requests",
      postBody = JsObject("orderBy" -> JsArray(JsObject("field" -> "pouet".toJson))).compactPrint,
      andExpect = BadRequest
    ).contentString should include("Cannot sort by `pouet`")
  }

  test("Any other *API* entry-point throws a 404") {
    server.httpGet(
      path = "/api/woot/woot",
      andExpect = NotFound
    )
  }

  test("Creating a deployment request with a too long name works but store a truncated user name") {
    val longUser = makeUser("too-long-user-name/" * 42)
    val longUserJWT = longUser.toJWT(authModule.jwtEncoder)

    val id = server.httpPost(
      path = s"/api/deployment-requests",
      headers = Map("Cookie" -> s"jwt=$longUserJWT"),
      andExpect = Created,
      postBody = Map(
        "productName" -> "my product",
        "version" -> "v2097",
        "target" -> "to everywhere"
      ).toJson.compactPrint
    ).contentString.parseJson.idAsLong

    getDeploymentRequest(id.toString).fields("creator").asInstanceOf[JsString].value shouldEqual
      "too-long-user-name/too-long-user-name/too-long-user-name/too-lon"
  }


  test("Any protected route responds 401 if the user is not logged in") {
    server.httpPost(
      path = s"/api/products",
      andExpect = Unauthorized,
      postBody = JsObject("name" -> JsString("this project will never be created")).compactPrint
    )
  }

  test("Any protected route responds 403 if the user is not allowed to do the operation") {
    server.httpPost(
      path = s"/api/products",
      headers = Map("Cookie" -> s"jwt=$stdUserJWT"),
      andExpect = Forbidden,
      postBody = JsObject("name" -> JsString("this project will never be created")).compactPrint
    )

    val depReqId = requestDeployment("my product", "version", "target".toJson)
    server.httpPost(
      path = s"/api/deployment-requests/$depReqId/actions/deploy",
      headers = Map("Cookie" -> s"jwt=$stdUserJWT"),
      andExpect = Forbidden,
      postBody = JsObject().compactPrint
    )
  }

}
