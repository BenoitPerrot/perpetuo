package com.criteo.perpetuo.app

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.auth.{User, UserFilter}
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.model.{ExecutionState, ProtoDeploymentPlanStep, ProtoDeploymentRequest, Version}
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

  private val config = AppConfigProvider.config
  private val authModule = new AuthModule(config.getConfig("auth"))
  private val productUser = makeUser("bob.the.producer")
  private val productUserJWT = productUser.toJWT(authModule.jwtEncoder)
  private val deployUser = makeUser("r.eleaser")
  private val deployUserJWT = deployUser.toJWT(authModule.jwtEncoder)
  private val stdUser = makeUser("stdUser")
  private val stdUserJWT = stdUser.toJWT(authModule.jwtEncoder)

  private var controller: RestController = _

  private val server = new EmbeddedHttpServer(new HttpServer {

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

  private val hrefHistory: mutable.Map[Long, JsValue] = mutable.Map()

  private def createProduct(name: String, expectedError: Option[(String, Status)] = None): Unit = {
    val ans = server.httpPut(
      path = s"/api/products",
      headers = Map("Cookie" -> s"jwt=$productUserJWT"),
      andExpect = expectedError.map(_._2).getOrElse(Created),
      putBody = JsObject("name" -> JsString(name)).compactPrint
    ).contentString
    expectedError.foreach(err => ans shouldEqual JsObject("errors" -> JsArray(JsString(err._1))).compactPrint)
  }

  private def requestAndWaitDeployment(productName: String, version: String, target: JsValue, comment: Option[String] = None, expectsMessage: Option[String] = None): Long = {
    val depReqId = requestDeployment(productName, version, target, comment, expectsMessage)
    expectsMessage.getOrElse {
      startDeploymentRequest(depReqId)
      getExecutionTracesByDeploymentRequestId(depReqId.toString).map(_.idAsLong).foreach(execTraceId =>
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
          "plan" -> JsArray(JsObject("target" -> target))
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
    ans should expectsMessage.map(include(_)).getOrElse(include regex """"id":\d+""")
    expectsMessage.map(_ => -1L).getOrElse(ans.parseJson.asJsObject.fields("id").asInstanceOf[JsNumber].value.toLongExact)
  }

  private def checkCreationDate(depReqMap: Map[String, JsValue]): Long = {
    depReqMap should contain key "creationDate"
    val creationDate = depReqMap("creationDate").toString.toLong
    creationDate should (be > 14e8.toLong and be < 20e8.toLong) // it's a timestamp in s.
    creationDate
  }

  private def expectState(id: Long, expectedState: String) = {
    server.httpGet(
      path = s"/api/deployment-requests/$id/state",
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsObject]
      .fields("state") shouldBe JsString(expectedState)
  }

  private def deepGetDepReq(id: Long) = {
    server.httpGet(
      path = s"/api/unstable/deployment-requests/$id",
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsObject]
  }

  private def deepGetDepReq(where: Seq[Map[String, JsValue]] = Seq(), limit: Option[Int] = None, offset: Option[Int] = None): Seq[Map[String, JsValue]] = {
    val q = JsObject(
      Map("where" -> where.toJson)
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
    actOnDeploymentRequest(deploymentRequestId, "step", JsObject(), expect)

  private def startDeploymentRequest(deploymentRequestId: Long): JsObject =
    startDeploymentRequest(deploymentRequestId, Ok).contentString.parseJson.asJsObject

  private val randomProductCounter = Iterator.from(1000)

  private def createProductAndStartDeployment(version: String, target: JsValue) = {
    val productName = s"random product ${randomProductCounter.next()}"
    createProduct(productName)
    val depReqId = requestAndWaitDeployment(productName, version, target)
    (depReqId, getExecutionTracesByDeploymentRequestId(depReqId.toString).map(_.idAsLong))
  }

  private def updateExecutionTrace(execTraceId: Long, state: String,
                                   href: Option[String],
                                   targetStatus: Option[Map[String, (String, String)]],
                                   executionDetail: Option[String] = None,
                                   expectedRequestStatus: Status = NoContent): Unit = {
    val params = Map(
      "state" -> Some(state.toJson),
      "href" -> href.map(_.toJson),
      "detail" -> executionDetail.map(_.toJson),
      "targetStatus" -> targetStatus.map(_.mapValues { case (s, d) => Map("code" -> s, "detail" -> d) }.toJson)
    ).collect {
      case (k, v) if v.isDefined => k -> v.get
    }
    putExecutionTrace(
      execTraceId.toString,
      params.toJson,
      expectedRequestStatus
    )
  }

  private def checkExecutionTraceUpdate(deploymentRequestId: Long,
                                        execTraceId: Long,
                                        state: String,
                                        href: Option[String] = None,
                                        targetStatus: Option[Map[String, (String, String)]] = None,
                                        expectedTargetStatus: Option[Map[String, (String, String)]] = None,
                                        executionDetail: Option[String] = None,
                                        expectedRequestStatus: Status = NoContent): Unit = {
    val previousHrefJson = hrefHistory.getOrElse(execTraceId, JsNull)
    val expectedHrefJson = href.map(_.toJson).getOrElse(previousHrefJson)
    hrefHistory(execTraceId) = expectedHrefJson

    updateExecutionTrace(execTraceId, state, href, targetStatus, executionDetail, expectedRequestStatus)

    val depReq = deepGetDepReq(deploymentRequestId)
    val operations = depReq.fields("operations").asInstanceOf[JsArray].elements.map(_.asJsObject.fields)
    operations.size shouldEqual 1
    val operation = operations.head

    expectedTargetStatus
      .orElse(targetStatus)
      .foreach(
        _.mapValues { case (s, d) => Map("code" -> s, "detail" -> d) }.toJson shouldEqual operation("targetStatus")
      )

    JsArray(
      JsObject(
        "id" -> execTraceId.toJson,
        "href" -> expectedHrefJson,
        "state" -> state.toJson,
        "detail" -> executionDetail.getOrElse("").toJson
      )
    ) shouldEqual operation("executions")

    val isRunning = state == ExecutionState.running.toString
    isRunning shouldEqual operation.get("closingDate").isEmpty
  }

  test("The Product's entry-point returns 201 when creating a Product") {
    createProduct("my product")
    createProduct("my other product")
  }

  test("The Product's entry-point returns the list of all known product names") {
    val products = server.httpGet(
      path = "/api/products",
      andExpect = Ok
    ).contentString.parseJson.asInstanceOf[JsArray].elements.map(_.asInstanceOf[JsObject].fields("name")
      .asInstanceOf[JsString].value)
    products should contain theSameElementsAs Seq("my product", "my other product")
  }

  test("The DeploymentRequest's POST entry-point returns 201 when creating a DeploymentRequest") {
    requestAndWaitDeployment("my product", "v21", "to everywhere".toJson, Some("my comment"))
    requestAndWaitDeployment("my other product", "buggy", "nowhere".toJson, None)
  }

  test("The DeploymentRequest's POST entry-point properly rejects bad input") {
    requestDeployment("{", Some("Unexpected end-of-input at input"))
    requestDeployment("""{"productName": "abc"}""", Some("no field named `version`"))
    requestDeployment("""{"productName": "abc", "version": "42"}""", Some("no field named `plan`"))
    requestDeployment("""{"productName": "abc", "plan": [{"target": "atom"}], "version": "2"}""", Some("Unknown product `abc`"))

    // rejected at serialization time:
    requestDeployment(s"""{"productName": "my product", "plan": [{"target": "atom"}], "version": "${"x" * 2000}"}""", Some("Too long version"))
  }

  test("The DeploymentRequest's POST entry-point handles a compound target expression") {
    requestAndWaitDeployment("my product", "42", Seq("here", "and", "there").toJson, Some(""))
  }

  test("The DeploymentRequest's POST entry-point properly rejects bad targets") {
    // just one case of bad target is tested here to check the controller's output, but parsing of targets is exhaustively tested in DeploymentRequestParserSpec
    requestAndWaitDeployment("my product", "b", JsArray(), None, Some("Unexpected target element: []"))
  }

  test("The DeploymentRequest's actions entry-point starts a deployment that was not started yet") {
    createProduct("my product B")
    val id = requestDeployment("my product B", "456", "ams".toJson)
    expectState(id, "notStarted")

    startDeploymentRequest(id) shouldEqual JsObject("id" -> id.toJson)
    expectState(id, "deployInProgress")
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
      case (k, n: JsNumber) => (k, n.prettyPrint)
      case (k, v) => (k, v.asInstanceOf[JsString].value)
    }

    createProduct("my new product")
    val id = requestDeployment("my new product", "789", "par".toJson)
    val respJson1 = getRespJson(actOnDeploymentRequest(id, "revert", JsObject(), UnprocessableEntity))
    respJson1("error") shouldEqual "Nothing to revert"
    respJson1 shouldNot contain("required")

    startDeploymentRequest(id, Ok)
    val execTraceId = getExecutionTracesByDeploymentRequestId(id.toString, Ok).get.head.idAsLong
    checkExecutionTraceUpdate(
      id, execTraceId, "completed", None,
      Some(Map("targetA" -> ("success", ""), "targetB" -> ("productFailure", ""))),
      Some(Map("targetA" -> ("success", ""), "targetB" -> ("productFailure", ""), "par" -> ("notDone", "")))
    )
    val respJson2 = getRespJson(actOnDeploymentRequest(id, "revert", JsObject(), UnprocessableEntity))
    respJson2("error") should startWith("a default rollback version is required")
    respJson2("required") shouldEqual "defaultVersion"

    val revertPlan = actOnDeploymentRequest(id, "devise-revert-plan", JsObject(), Ok).contentString.parseJson
    revertPlan shouldEqual Map(
      "undetermined" -> Seq("targetB", "targetA"),
      "determined" -> Seq()
    ).toJson

    actOnDeploymentRequest(id, "revert", JsObject("defaultVersion" -> JsString("42"), "operationCount" -> JsNumber(1)), Ok)
  }

  private def getExecutionTracesByDeploymentRequestId(deploymentRequestId: String, expectedStatus: Status): Option[Vector[JsValue]] = {
    val response = server.httpGet(
      path = s"/api/unstable/deployment-requests/$deploymentRequestId",
      andExpect = expectedStatus
    )
    if (response.status == Ok) {
      Some(
        response.contentString.parseJson.asInstanceOf[JsObject]
          .fields("operations").asInstanceOf[JsArray].elements
          .flatMap(_.asInstanceOf[JsObject].fields("executions").asInstanceOf[JsArray].elements)
      )
    } else
      None
  }

  private def getExecutionTracesByDeploymentRequestId(deploymentRequestId: String): Vector[JsValue] =
    getExecutionTracesByDeploymentRequestId(deploymentRequestId, Ok).get

  test("The DeploymentRequest's GET entry-point returns 200 and a JSON with all necessary info when accessing an existing DeploymentRequest") {
    val depReqId = requestAndWaitDeployment("my product", "v2097", "to everywhere".toJson, Some("hello world"))

    val values1 = deepGetDepReq(depReqId).fields

    checkCreationDate(values1)

    Map(
      "id" -> JsNumber(depReqId),
      "productName" -> JsString("my product"),
      "version" -> JsString("v2097"),
      "plan" -> JsArray(
        JsObject("id" -> T, "name" -> JsString(""), "targetExpression" -> JsString("to everywhere"), "comment" -> JsString(""))
      ),
      "comment" -> JsString("hello world"),
      "creator" -> JsString("r.eleaser"),
      "creationDate" -> T
    ).map { case (k, v) =>
      v shouldEqual values1(k)
    }
  }

  test("The DeploymentRequest's GET entry-point returns 200 and a JSON with the same target expression as provided") {
    val plan = deepGetDepReq(3).fields("plan")
    plan.asInstanceOf[JsArray].elements
      .map { step =>
        step.asInstanceOf[JsObject].fields("targetExpression").asInstanceOf[JsArray].elements
          .map { el =>
            el.asInstanceOf[JsString].value
          }
      } shouldEqual Vector(Vector("here", "and", "there"))
  }

  test("The ExecutionTrace's entry-point returns 404 when trying to access a non-existing DeploymentRequest") {
    getExecutionTracesByDeploymentRequestId("4242", NotFound)
  }

  test("The ExecutionTrace's entry-point doesn't fail when the existing DeploymentRequest doesn't have execution traces yet") {
    val protoDeploymentRequest = ProtoDeploymentRequest("my product", Version(JsString("v")), Seq(ProtoDeploymentPlanStep("", JsString("t"), "")), "c", "c")
    val depReq = Await.result(controller.engine.crankshaft.createDeploymentRequest(protoDeploymentRequest), 1.second)
    val traces = getExecutionTracesByDeploymentRequestId(depReq.id.toString)
    traces shouldBe empty
  }

  test("The ExecutionTrace's entry-point returns a list of executions when trying to access a completed DeploymentRequest") {
    val traces = getExecutionTracesByDeploymentRequestId("1")
    traces.length shouldEqual 1
    Map(
      "id" -> T,
      "href" -> JsNull,
      "state" -> "completed".toJson,
      "detail" -> "".toJson
    ) shouldEqual traces.head.asJsObject.fields
  }

  test("The ExecutionTrace's entry-point updates one record's execution state on a PUT") {
    val (depReqId, executionTraces) = createProductAndStartDeployment("1112", "paris".toJson)
    checkExecutionTraceUpdate(depReqId, executionTraces.head, "completed", executionDetail = Some("execution detail"))
  }

  test("The ExecutionTrace's entry-point updates one record's execution state and href on a PUT") {
    val (depReqId, executionTraces) = createProductAndStartDeployment("1112", "paris".toJson)
    checkExecutionTraceUpdate(depReqId, executionTraces.head, "completed", Some("http://somewhe.re"))
  }

  test("The ExecutionTrace's entry-point updates one record's execution state and target status on a PUT") {
    val productName = s"random product ${randomProductCounter.next()}"
    createProduct(productName)
    val depReqId = requestDeployment(productName, "653", Seq("paris", "ams").toJson)
    startDeploymentRequest(depReqId)
    getExecutionTracesByDeploymentRequestId(depReqId.toString).map(_.idAsLong).foreach(
      checkExecutionTraceUpdate(
        depReqId, _, "initFailed", None,
        Some(Map("paris" -> ("success", ""))),
        Some(Map("paris" -> ("success", ""), "ams" -> ("notDone", "")))
      )
    )
  }

  test("The ExecutionTrace's entry-point updates one record's execution state, href and target status (partially) on a PUT") {
    val depReqId = requestDeployment("my product", "653", Seq("paris", "amsterdam").toJson, None)
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString)(0).idAsLong
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "conflicting", Some("http://"),
      Some(Map("amsterdam" -> ("notDone", "foo"))),
      Some(Map("amsterdam" -> ("notDone", "foo"), "paris" -> ("notDone", "")))
    )
  }

  test("The ExecutionTrace's entry-point partially updates one record's target status on a PUT") {
    val depReqId = requestDeployment("my product", "653", Seq("paris", "amsterdam").toJson, None)
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString)(0).idAsLong
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "running", None,
      Some(Map("amsterdam" -> ("notDone", ""))),
      Some(Map("amsterdam" -> ("notDone", "pending"), "paris" -> ("notDone", "pending")))
    )
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "running", None,
      Some(Map("amsterdam" -> ("running", ""), "paris" -> ("notDone", "waiting...")))
    )
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "aborted", Some("http://final"),
      Some(Map(
        "tokyo" -> ("notDone", "crashed"),
        "london" -> ("running", "am I too late?")
      )),
      Some(Map(
        "paris" -> ("notDone", "waiting..."), // untouched; example of a case where the detail doesn't make sense anymore (while it still might)
        "amsterdam" -> ("undetermined", "No feedback from the executor"), // auto-updated on close
        "tokyo" -> ("notDone", "crashed"), // "manually" updated on close
        "london" -> ("undetermined", "No feedback from the executor") // "manually" but then automatically updated on close
      ))
    )
  }

  test("The ExecutionTrace's entry-point rejects impossible transitions") {
    createProduct("calm-camel")
    val depReqId = requestDeployment("calm-camel", "123456", Seq("paris", "amsterdam").toJson, None)
    startDeploymentRequest(depReqId)
    val execTraceId = getExecutionTracesByDeploymentRequestId(depReqId.toString)(0).idAsLong
    checkExecutionTraceUpdate(
      depReqId, execTraceId, "conflicting", None,
      Some(Map("amsterdam" -> ("notDone", ""))),
      Some(Map("paris" -> ("notDone", ""), "amsterdam" -> ("notDone", "")))
    )
    updateExecutionTrace(
      execTraceId, "completed", Some("http://final"),
      Some(Map("paris" -> ("hostFailure", "crashed"))), None,
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
    expectState(depReqId, "deployFlopped")
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

    actOnDeploymentRequest(depReqId, "stop", JsObject("operationCount" -> JsNumber(0)), Status.Conflict)
    val executionTraceIds = getExecutionTracesByDeploymentRequestId(depReqId.toString).map(_.idAsLong)

    actOnDeploymentRequest(depReqId, "stop", JsObject("operationCount" -> JsNumber(1)), Status.Ok).contentString.parseJson.asJsObject shouldEqual
      JsObject(
        "id" -> JsNumber(depReqId),
        "stopped" -> JsNumber(0),
        "failures" -> executionTraceIds.map(id => s"No href for execution trace #$id, thus cannot interact with the actual execution").toJson
      )

    executionTraceIds.foreach(execTraceId =>
      updateExecutionTrace(
        execTraceId, "running", Some("http://final"),
        Some(Map("paris" -> ("running", "")))
      )
    )
    actOnDeploymentRequest(depReqId, "stop", JsObject("operationCount" -> JsNumber(1)), Status.Ok).contentString.parseJson.asJsObject shouldEqual
      JsObject(
        "id" -> JsNumber(depReqId),
        "stopped" -> JsNumber(0),
        "failures" -> JsArray(JsString("Could not find an execution configuration for the type `unknown`"))
      )

    executionTraceIds.foreach(execTraceId =>
      updateExecutionTrace(
        execTraceId, "aborted", Some("http://final"), None
      )
    )
    actOnDeploymentRequest(depReqId, "stop", JsObject("operationCount" -> JsNumber(1)), Status.Ok).contentString.parseJson.asJsObject shouldEqual
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
        "paris" -> ("success", ""),
        "amsterdam" -> ("hostFailure", "some interesting details")
      ))
    )

    val depReq = deepGetDepReq(depReqId)
    expectState(depReqId, "deployFailed")

    JsArray(
      JsObject(
        "id" -> T,
        "planStepIds" -> JsArray(T),
        "kind" -> "deploy".toJson,
        "creator" -> "r.eleaser".toJson,
        "creationDate" -> T,
        "closingDate" -> T,
        "status" -> "failed".toJson,
        "targetStatus" -> Map(
          "paris" -> Map("code" -> "success", "detail" -> "").toJson,
          "amsterdam" -> Map("code" -> "hostFailure", "detail" -> "some interesting details").toJson
        ).toJson,
        "executions" -> JsArray(
          JsObject(
            "id" -> execTraceId.toJson,
            "href" -> "http://final".toJson,
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
        "planStepIds" -> JsArray(T),
        "kind" -> "deploy".toJson,
        "creator" -> "r.eleaser".toJson,
        "creationDate" -> T,
        "status" -> "inProgress".toJson,
        "targetStatus" -> Map("tokyo" -> Map("code" -> "notDone", "detail" -> "pending")).toJson,
        "executions" -> JsArray(
          JsObject(
            "id" -> T,
            "href" -> JsNull,
            "state" -> "pending".toJson,
            "detail" -> "".toJson
          )
        )
      )
    ) shouldEqual delayedDepReq.fields("operations")
    expectState(delayedDepReqId, "deployInProgress")
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

  test("Deep query sorts by deployment request ID (descending)") {
    val sortedDepReqs = deepGetDepReq()
    sortedDepReqs.length should be > 2
    sortedDepReqs.foldLeft(Int.MaxValue) { (lastId, deploymentRequest) =>
      val id = deploymentRequest("id").asInstanceOf[JsNumber].value.toIntExact
      lastId should be > id
      id
    }
  }

  test("Any other *API* entry-point throws a 404") {
    server.httpGet(
      path = "/api/woot/woot",
      andExpect = NotFound
    )
  }

  test("Creating a deployment request with a too long name works but stores an explicitly truncated user name") {
    val longUser = makeUser("too-long-user-name/" * 42)
    val longUserJWT = longUser.toJWT(authModule.jwtEncoder)

    val id = server.httpPost(
      path = s"/api/deployment-requests",
      headers = Map("Cookie" -> s"jwt=$longUserJWT"),
      andExpect = Created,
      postBody = JsObject(
        "productName" -> JsString("my product"),
        "version" -> JsString("v2097"),
        "plan" -> JsArray(JsObject("target" -> JsString("to everywhere")))
      ).compactPrint
    ).contentString.parseJson.idAsLong

    deepGetDepReq(id).fields("creator").asInstanceOf[JsString].value shouldEqual
      "too-long-user-name/too-long-user-name/too-long-user-name/too-..."
  }


  test("Any protected route responds 401 if the user is not logged in") {
    server.httpPut(
      path = s"/api/products",
      andExpect = Unauthorized,
      putBody = JsObject("name" -> JsString("this project will never be created")).compactPrint
    )
  }

  test("Any protected route responds 403 if the user is not allowed to do the operation") {
    server.httpPut(
      path = s"/api/products",
      headers = Map("Cookie" -> s"jwt=$stdUserJWT"),
      andExpect = Forbidden,
      putBody = JsObject("name" -> JsString("this project will never be created")).compactPrint
    )

    val depReqId = requestDeployment("my product", "version", "target".toJson)
    server.httpPost(
      path = s"/api/deployment-requests/$depReqId/actions/step",
      headers = Map("Cookie" -> s"jwt=$stdUserJWT"),
      andExpect = Forbidden,
      postBody = JsObject().compactPrint
    )
  }

}
