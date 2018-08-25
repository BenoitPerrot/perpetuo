package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.TargetTerm
import com.criteo.perpetuo.model.Version
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.inject.Test
import com.twitter.util.Future
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._


class RundeckTriggerSpec extends Test {

  private class TriggerMock(statusMock: Int, contentMock: String) extends RundeckTrigger("rundeck", "localhost", "perpetuo-deployment") {

    private class RundeckClientMock extends RundeckClient(host) {
      override val authToken: Option[String] = Some("my-super-secret-token")

      override protected val client: Request => Future[Response] = (request: Request) => {
        request.uri shouldEqual s"/api/16/job/perpetuo-deployment/executions?authtoken=my-super-secret-token"
        request.contentString shouldEqual """{"argString":"-callback-url 'http://somewhere/api/execution-traces/42' -product-name 'My\"Beautiful\"Project' -target 'a,b' -product-version \"the 042nd version\""}"""
        val resp = Response(Status(statusMock))
        resp.write(contentMock)
        Future.value(resp)
      }
    }

    override protected val client: RundeckClient = new RundeckClientMock

    def testTrigger: Option[String] = {
      val productName = "My\"Beautiful\"Project"
      val version = Version(JsString("the 042nd version"))
      val target = Set(TargetTerm(Set(JsObject("abc" -> JsString("def"), "ghi" -> JsNumber(51.3))), Set("a", "b")))
      Await.result(trigger(42, productName, version, target, "guy next door"), 1.second)
    }
  }

  test("Rundeck's API is followed when everything goes well") {
    new TriggerMock(200, """{"id": 123, "permalink": "http://rundeck/job/123/show"}""").testTrigger shouldEqual Some("http://rundeck/job/123/show")
  }

  test("Rundeck's API is followed when a connection problem occurs") {
    val exc = the[Exception] thrownBy new TriggerMock(403, "<html>gibberish</html>").testTrigger
    exc.getMessage shouldEqual "Bad response from rundeck (job: perpetuo-deployment): Forbidden"
  }

  test("Rundeck's API is followed when an internal server error occurs") {
    val exc = the[Exception] thrownBy new TriggerMock(500, "<html><p>Intelligible error</p></html>").testTrigger
    exc.getMessage should endWith("Internal Server Error: Intelligible error")
  }

  test("Rundeck's API is followed when the request cannot be satisfied") {
    val exc = the[Exception] thrownBy new TriggerMock(400, """{"error": true, "message": "Intelligible error"}""").testTrigger
    exc.getMessage should endWith("""Bad Request: Intelligible error""")
  }
}
