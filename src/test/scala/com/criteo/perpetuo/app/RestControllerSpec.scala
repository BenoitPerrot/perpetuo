package com.criteo.perpetuo.app

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.auth.{User, UserFilter}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, Version}
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

  val config = AppConfigProvider.config
  val authModule = new AuthModule(config.getConfig("auth"))
  val productUser = User("bob.the.producer")
  val productUserJWT = productUser.toJWT(authModule.jwtEncoder)
  val deployUser = User("r.eleaser")
  val deployUserJWT = deployUser.toJWT(authModule.jwtEncoder)
  val stdUser = User("stdUser")
  val stdUserJWT = stdUser.toJWT(authModule.jwtEncoder)
  val deprecatedDeployUser = User("qabot") // todo: remove once the deprecated route for starting deployments is removed
  val deprecatedDeployUserJWT = deprecatedDeployUser.toJWT(authModule.jwtEncoder)

  var controller: RestController = _

  val server = new EmbeddedHttpServer(new HttpServer {

    override protected def jacksonModule: FinatraJacksonModule = CustomServerModules.jackson

    override def modules = Seq(
      authModule,
      new PluginsModule(config),
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

  private def createProduct(name: String, expectedError: Option[(String, Status)] = None) = {
    val ans = server.httpPost(
      path = s"/api/products",
      headers = Map("Cookie" -> s"jwt=$productUserJWT"),
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

  private def httpPut(path: String, body: JsValue, expect: Status) =
    server.httpPut(
      path = path,
      headers = Map("Cookie" -> s"jwt=$deprecatedDeployUserJWT"),
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

  private var randomProductCounter = 1000

  private def createAndStartDeployment(version: String, target: JsValue) = {
    randomProductCounter += 1
    val productName = s"random product $randomProductCounter"
    createProduct(productName)
    val depReqId = requestDeployment(productName, version, target, None, start = false).idAsLong
    startDeploymentRequest(depReqId)
    (depReqId, getExecutionTracesByDeploymentRequestId(depReqId.toString).elements.map(_.idAsLong))
  }

  private def updateExecTrace(deploymentRequestId: Long, execTraceId: Long, state: String, logHref: Option[String],
                              targetStatus: Option[Map[String, JsValue]] = None,
                              executionDetail: Option[String] = None,
                              expectedTargetStatus: Map[String, (String, String)]): Unit = {
    val logHrefJson = logHref.map(_.toJson)
    val previousLogHrefJson = logHrefHistory.getOrElse(execTraceId, JsNull)
    val expectedLogHrefJson = logHrefJson.getOrElse(previousLogHrefJson)
    logHrefHistory(execTraceId) = expectedLogHrefJson

    val params = Map(
      "state" -> Some(state.toJson),
      "logHref" -> logHrefJson,
      "detail" -> executionDetail.map(_.toJson),
      "targetStatus" -> targetStatus.map(_.toJson)
    ).collect {
      case (k, v) if v.isDefined => k -> v.get
    }
    val ret = httpPut(
      RestApi.executionCallbackPath(execTraceId.toString),
      params.toJson,
      NoContent
    )
    ret.contentLength shouldEqual Some(0)

    val depReq = deepGetDepReq(deploymentRequestId)

    val operations = depReq.fields("operations").asInstanceOf[JsArray].elements.map(_.asJsObject.fields)
    operations.size shouldEqual 1
    Map(
      "id" -> T,
      "kind" -> "deploy".toJson,
      "creator" -> "r.eleaser".toJson,
      "creationDate" -> T,
      "closingDate" -> T,
      "targetStatus" -> expectedTargetStatus.mapValues { case (s, d) => Map("code" -> s, "detail" -> d) }.toJson,
      "executions" -> JsArray(
        JsObject(
          "id" -> execTraceId.toJson,
          "logHref" -> expectedLogHrefJson,
          "state" -> state.toJson,
          "detail" -> executionDetail.getOrElse("").toJson
        )
      )
    ) shouldEqual operations.head
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
  }

  test("The Product's entry-points returns 201 when creating a Product") {
    createProduct("my product")
    createProduct("my other product")
  }

  test("The Product's entry-points properly rejects already used names") {
    createProduct("my product", Some("Name `my product` is already used", Conflict))
  }

  test("The Product's entry-points returns the list of all known product names") {
    val products = server.httpGet(
      path = "/api/products",
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsString].value)
    products should contain theSameElementsAs Seq("my product", "my other product")
  }

  test("The DeploymentRequest's POST entry-point returns 201 when creating a DeploymentRequest") {
    requestDeployment("my product", "v21", "to everywhere".toJson, Some("my comment"))
    requestDeployment("my other product", "buggy", "nowhere".toJson, None)
  }

  test("The DeploymentRequest's POST entry-point properly rejects bad input") {
    requestDeployment("{", Some("Unexpected end-of-input at input"), start = true)
    requestDeployment("""{"productName": "abc"}""", Some("Expected to find `version`"), start = true)
    requestDeployment("""{"productName": "abc", "version": "42"}""", Some("Expected to find `target`"), start = true)
    requestDeployment("""{"productName": "abc", "target": "*", "version": "2"}""", Some("Product `abc` could not be found"), start = true)
  }

  test("The DeploymentRequest's POST entry-point handles a complex target expression") {
    requestDeployment("my product", "42", Seq("here", "and", "there").toJson, Some(""))
    requestDeployment("my product", " 10402", Map("select" -> "here").toJson, Some(""))
    requestDeployment("my product", "0042", Map("select" -> Seq("here", "and", "there")).toJson, Some(""))
    requestDeployment("my product", "420", Seq(Map("select" -> Seq("here", "and", "there"))).toJson, Some(""))
  }

  test("The DeploymentRequest's POST entry-point properly rejects bad targets") {
    requestDeployment("my product", "b", JsArray(), None, Some("non-empty JSON array or object"))
    requestDeployment("my product", "b", JsObject(), None, Some("must contain a field `select`"))
    requestDeployment("my product", "b", 60.toJson, None, Some("JSON array or object"))
    requestDeployment("my product", "b", Seq(42).toJson, None, Some("JSON object or string"))
    requestDeployment("my product", "b", Seq(JsObject()).toJson, None, Some("must contain a field `select`"))
    requestDeployment("my product", "b", Seq(Map("select" -> 42)).toJson, None, Some("non-empty JSON string or array"))
    requestDeployment("my product", "b", Seq(Map("select" -> Seq(42))).toJson, None, Some("a JSON string in"))
  }

  test("The DeploymentRequest's actions entry-point starts a deployment that was not started yet (using deprecated API)") {
    createProduct("my product A")
    val id = requestDeployment("my product A", "not ready yet", "par".toJson, None, None, start = false).idAsLong
    httpPut(
      s"/api/deployment-requests/$id",
      "".toJson,
      Ok
    ).contentString.parseJson.asJsObject shouldEqual JsObject("id" -> id.toJson)
  }

  test("The DeploymentRequest's actions entry-point returns 404 when trying to start a non-existing DeploymentRequest (using deprecated API)") {
    httpPut(
      "/api/deployment-requests/4242",
      "".toJson,
      NotFound
    )
  }

  test("The DeploymentRequest's actions entry-point starts a deployment that was not started yet") {
    createProduct("my product B")
    val id = requestDeployment("my product B", "456", "ams".toJson, None, None, start = false).idAsLong
    startDeploymentRequest(id.longValue()) shouldEqual JsObject("id" -> id.toJson)
  }

  test("The DeploymentRequest's actions entry-point returns 404 when trying to start a non-existing DeploymentRequest") {
    startDeploymentRequest(4242, NotFound)
  }

  test("The DeploymentRequest's actions entry-point returns 400 when posting a non-existing action") {
    actOnDeploymentRequest(1, "ploup", JsObject(), BadRequest)
  }

  test("The DeploymentRequest's actions entry-point returns 422 when trying to revert new targets") {
    val getRespJson = (_: Response).contentString.parseJson.asJsObject.fields.map {
      case ("errors", v: JsArray) => ("error", v.elements.head.asInstanceOf[JsString].value)
      case (k, v) => (k, v.asInstanceOf[JsString].value)
    }

    createProduct("my new product")
    val id = requestDeployment("my new product", "789", "par".toJson, None, None, start = false).idAsLong
    val respJson1 = getRespJson(actOnDeploymentRequest(id, "revert", JsObject(), UnprocessableEntity))
    respJson1("error") should include("it has not yet been applied")
    respJson1 shouldNot contain("required")

    startDeploymentRequest(id, Ok)
    val execTraceId = getExecutionTracesByDeploymentRequestId(id.toString, Ok).get.elements.head.idAsLong
    updateExecTrace(
      id, execTraceId, "completed", None, Some(
        Map("targetA" -> Map("code" -> "success", "detail" -> "").toJson,
          "targetB" -> Map("code" -> "productFailure", "detail" -> "").toJson)),
      expectedTargetStatus = Map("targetA" -> ("success", ""), "targetB" -> ("productFailure", ""))
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
    val o = requestDeployment("my product", "v2097", "to everywhere".toJson, Some("hello world"))
    val i = o.idAsLong

    val values1 = getDeploymentRequest(i.toString).fields

    checkCreationDate(values1)

    Map(
      "id" -> JsNumber(i),
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

  test("The ExecutionTrace's entry-points returns 404 when trying to access a non-existing DeploymentRequest") {
    getExecutionTracesByDeploymentRequestId("4242", NotFound)
  }

  test("The ExecutionTrace's entry-points not fails when the existing DeploymentRequest doesn't have execution traces yet") {
    val attrs = new DeploymentRequestAttrs("my product", Version("\"v\""), "\"t\"", "c", "c", new Timestamp(System.currentTimeMillis))
    val depReq = Await.result(controller.engine.createDeploymentRequest(attrs, immediateStart = false), 1.second)
    val traces = getExecutionTracesByDeploymentRequestId(depReq("id").toString).elements
    traces shouldBe empty
  }

  test("The ExecutionTrace's entry-points returns a list of executions when trying to access an existing DeploymentRequest") {
    val traces = getExecutionTracesByDeploymentRequestId("1").elements
    traces.length shouldEqual 1
    Map(
      "id" -> T,
      "logHref" -> JsNull,
      "state" -> "pending".toJson,
      "detail" -> "".toJson
    ) shouldEqual traces.head.asJsObject.fields
  }

  test("The ExecutionTrace's entry-points updates one record's execution state on a PUT") {
    val (depReqId, executionTraces) = createAndStartDeployment("1112", "paris".toJson)
    updateExecTrace(
      depReqId, executionTraces.head, "completed", None, None, Some("execution detail"),
      expectedTargetStatus = Map()
    )
  }

  test("The ExecutionTrace's entry-points updates one record's execution state and log href on a PUT") {
    val (depReqId, executionTraces) = createAndStartDeployment("1112", "paris".toJson)
    updateExecTrace(
      depReqId, executionTraces.head, "completed", Some("http://somewhe.re"),
      expectedTargetStatus = Map()
    )
  }

  test("The ExecutionTrace's entry-points updates one record's execution state and target status on a PUT") {
    val (depReqId, executionTraces) = createAndStartDeployment("653", Seq("paris", "ams").toJson)
    updateExecTrace(
      depReqId, executionTraces.head, "initFailed", None,
      targetStatus = Some(Map("paris" -> Map("code" -> "success", "detail" -> "").toJson)),
      expectedTargetStatus = Map("paris" -> ("success", ""))
    )
  }

  // todo: restore once Engine supports partial update
  /*
  test("The ExecutionTrace's entry-points updates one record's execution state, log href and target status (partially) on a PUT") {
    val depReqId = requestDeployment("my product", "653", Seq("paris", "amsterdam").toJson, None, start = false).idAsLong
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString).elements(0).idAsLong
    updateExecTrace(
      depReqId, execTraceId, "conflicting", Some("http://"),
      targetStatus = Some(Map("amsterdam" -> Map("code" -> "notDone", "detail" -> "").toJson)),
      expectedTargetStatus = Map("amsterdam" -> ("notDone", ""))
    )
  }

  test("The ExecutionTrace's entry-points partially updates one record's target status on a PUT") {
    val depReqId = requestDeployment("my product", "653", Seq("paris", "amsterdam").toJson, None, start = false).idAsLong
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString).elements(0).idAsLong
    updateExecTrace(
      depReqId, execTraceId, "conflicting", None,
      targetStatus = Some(Map("amsterdam" -> Map("code" -> "notDone", "detail" -> "").toJson)),
      expectedTargetStatus = Map("amsterdam" -> ("notDone", ""))
    )
    updateExecTrace(
      depReqId, execTraceId, "completed", Some("http://final"),
      targetStatus = Some(Map("amsterdam" -> Map("code" -> "hostFailure", "detail" -> "some details...").toJson)),
      expectedTargetStatus = Map("paris" -> ("success", ""), "amsterdam" -> ("hostFailure", "some details..."))
    )
  }
  */

  test("The ExecutionTrace's entry-points returns 404 on non-integral ID") {
    httpPut(
      RestApi.executionCallbackPath("abc"),
      Map("state" -> "conflicting").toJson,
      NotFound
    )
  }

  test("The ExecutionTrace's entry-points returns 404 if trying to update an unknown execution trace") {
    httpPut(
      RestApi.executionCallbackPath("12345"),
      Map("state" -> "conflicting").toJson,
      NotFound
    )
  }

  test("The ExecutionTrace's entry-points returns 400 if the target status is badly formatted and not update the state") {
    val (depReqId, executionTraces) = createAndStartDeployment("2456", Seq("paris", "amsterdam").toJson)
    val execTraceId = executionTraces.head

    httpPut(
      RestApi.executionCallbackPath(execTraceId.toString),
      JsObject("state" -> "conflicting".toJson, "targetStatus" -> Vector("paris", "amsterdam").toJson),
      BadRequest
    ).contentString should include("targetStatus: Can not deserialize")

    updateExecTrace(
      depReqId, execTraceId, "completed", None,
      expectedTargetStatus = Map()
    )
  }

  test("The ExecutionTrace's entry-points returns 400 if no state is provided") {
    httpPut(
      RestApi.executionCallbackPath("1"),
      JsObject("targetStatus" -> Map("par" -> "success").toJson),
      BadRequest
    ).contentString should include("state: field is required")
  }

  test("The ExecutionTrace's entry-points returns 400 if the provided state is unknown") {
    val (_, executionTraces) = createAndStartDeployment("2456", Seq("paris", "amsterdam").toJson)

    httpPut(
      RestApi.executionCallbackPath(executionTraces.head.toString),
      Map("state" -> "what?").toJson,
      BadRequest
    ).contentString should include("Unknown state `what?`")
  }

  test("The ExecutionTrace's entry-points returns 400 and not update the state if a provided target status is unknown") {
    val (depReqId, executionTraces) = createAndStartDeployment("2456", Seq("paris", "amsterdam").toJson)
    val execTraceId = executionTraces.head

    httpPut(
      RestApi.executionCallbackPath(execTraceId.toString),
      JsObject("state" -> "conflicting".toJson, "targetStatus" -> Map("par" -> Map("code" -> "foobar", "detail" -> "")).toJson),
      BadRequest
    ).contentString should include("Bad target status for `par`: code='foobar', detail=''")

    updateExecTrace(
      depReqId, execTraceId, "completed", None,
      expectedTargetStatus = Map()
    )
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
    val depReqId = requestDeployment("my product", "8080", "paris".toJson, None, start = false).idAsLong

    val depReq = deepGetDepReq(depReqId)
    depReq.fields("version").asInstanceOf[JsString].value shouldEqual "8080"
  }

  test("Deep query returns the right executions in a valid JSON") {
    val (depReqId, executionTraces) = createAndStartDeployment("3211", Seq("paris", "amsterdam").toJson)
    val execTraceId = executionTraces.head
    updateExecTrace(
      depReqId, execTraceId, "completed", Some("http://final"),
      targetStatus = Some(Map(
        "paris" -> Map("code" -> "success", "detail" -> "").toJson,
        "amsterdam" -> Map("code" -> "hostFailure", "detail" -> "some interesting details").toJson)),
      expectedTargetStatus = Map(
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
    val delayedDepReqId = requestDeployment("my product D", "5133", "tokyo".toJson, None, start = false).idAsLong
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
    val longUser = User("too-long-user-name/" * 42)
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

}
