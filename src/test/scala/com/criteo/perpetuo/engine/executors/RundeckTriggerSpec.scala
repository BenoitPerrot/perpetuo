package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.model.{TargetAtom, Version}
import com.criteo.perpetuo.util.{ConsumedResponse, SingleNodeHttpClient}
import com.twitter.finagle.http.{Request, Status}
import com.twitter.inject.Test
import com.twitter.io.Buf.Utf8
import com.twitter.util.{Duration, Future}
import com.typesafe.config.ConfigFactory
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._


class RundeckTriggerSpec extends Test {
  private val executionCallbackUrl = "http://somewhere/api/execution-traces/42"

  private def testTrigger(statusMock: Int, contentMock: String) = {
    val clientMock = new SingleNodeHttpClient("localhost", Duration.Top) {
      override def apply(request: Request, isIdempotent: Boolean = false): Future[ConsumedResponse] = {
        request.uri shouldEqual s"/api/16/job/perpetuo-deployment/executions?authtoken=my-super-secret-token"
        request.contentString shouldEqual s"""{"argString":"-callback-url '$executionCallbackUrl' -product-name 'My\\"Beautiful\\"Project' -target 'a,b' -product-version \\"the 042nd version\\""}"""
        Future.value(ConsumedResponse(Status(statusMock), Utf8(contentMock), "rundeck"))
      }
    }
    val rundeckTrigger = new RundeckTrigger(new RundeckClient(clientMock, Some("my-super-secret-token")), "perpetuo-deployment")
    val productName = "My\"Beautiful\"Project"
    val version = Version(JsString("the 042nd version"))
    val target = TargetAtomSet(Set.empty, Set("a", "b").map(TargetAtom))
    Await.result(rundeckTrigger.trigger(executionCallbackUrl, productName, version, target, "guy next door"), 1.second)
  }

  test("Rundeck's API is followed when everything goes well") {
    testTrigger(200, """{"id": 123, "permalink": "http://rundeck/job/123/show"}""") shouldEqual Some("http://rundeck/job/123/show")
  }

  test("Rundeck's API is followed when a connection problem occurs") {
    val exc = the[Exception] thrownBy testTrigger(403, "<html>gibberish</html>")
    exc.getMessage shouldEqual "Bad response from Rundeck (on localhost) (job: perpetuo-deployment): Forbidden"
  }

  test("Rundeck's API is followed when an internal server error occurs") {
    val exc = the[Exception] thrownBy testTrigger(500, "<html><p>Intelligible error</p></html>")
    exc.getMessage should endWith("Internal Server Error: Intelligible error")
  }

  test("Rundeck's API is followed when the request cannot be satisfied") {
    val exc = the[Exception] thrownBy testTrigger(400, """{"error": true, "message": "Intelligible error"}""")
    exc.getMessage should endWith("""Bad Request: Intelligible error""")
  }

  test("RundeckTrigger must be loadable by configuration, to be used with the singleExecutor dispatcher") {
    new RundeckTrigger(ConfigFactory.parseString(
      """
        |host = "foo-01.criteo.com"
        |jobName = "deploy-foo"
      """.stripMargin))
  }
}
