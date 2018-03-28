package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.engine.TargetTerm
import com.criteo.perpetuo.model.Version
import com.twitter.finagle.http.{Response, Status}
import com.twitter.inject.Test
import com.twitter.util.Future
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._


class RundeckTriggerSpec extends Test with TestDb {
  private def testWhenResponseIs(statusCode: Int, content: String) = {
    val rundeckTrigger = new RundeckTrigger("rundeck", "localhost", 4440, "my-super-secret-token", "perpetuo-deployment")
    assert(rundeckTrigger.getClass.getSimpleName == "RundeckTrigger")
    rundeckTrigger.client = request => {
      request.uri shouldEqual s"/api/16/job/perpetuo-deployment/executions?authtoken=my-super-secret-token"
      request.contentString shouldEqual """{"argString":"-callback-url 'http://somewhere/api/execution-traces/42' -product-name 'My\"Beautiful\"Project' -target 'a,b' -product-version \"the 042nd version\""}"""
      val resp = Response(Status(statusCode))
      resp.write(content)
      Future.value(resp)
    }
    val productName = "My\"Beautiful\"Project"
    val version = Version("\"the 042nd version\"")
    val target = Set(TargetTerm(Set(JsObject("abc" -> JsString("def"), "ghi" -> JsNumber(51.3))), Set("a", "b")))
    val logHref = Await.result(rundeckTrigger.trigger(42, productName, version, target, "guy next door"), 1.second)
    logHref shouldBe defined
    logHref.get
  }

  test("Rundeck's API is followed when everything goes well") {
    testWhenResponseIs(200, """{"id": 123, "permalink": "http://rundeck/job/123/show"}""") shouldEqual "http://rundeck/job/123/show"
  }

  test("Rundeck's API is followed when a connection problem occurs") {
    val exc = the[Exception] thrownBy testWhenResponseIs(403, "<html>gibberish</html>")
    exc.getMessage shouldEqual "Bad response from rundeck (job: perpetuo-deployment): Forbidden"
  }

  test("Rundeck's API is followed when an internal server error occurs") {
    val exc = the[Exception] thrownBy testWhenResponseIs(500, "<html><p>Intelligible error</p></html>")
    exc.getMessage should endWith("Internal Server Error: Intelligible error")
  }

  test("Rundeck's API is followed when the request cannot be satisfied") {
    val exc = the[Exception] thrownBy testWhenResponseIs(400, """{"error": true, "message": "Intelligible error"}""")
    exc.getMessage should endWith("""Bad Request: Intelligible error""")
  }
}
