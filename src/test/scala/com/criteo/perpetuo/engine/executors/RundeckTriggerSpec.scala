package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
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

  private class TriggerMock(statusMock: Int, contentMock: String) extends RundeckTrigger("localhost", "perpetuo-deployment") {

    private val rundeckConfig = AppConfig.executorConfig("rundeck")

    private class RundeckClientMock extends RundeckClient(host, rundeckConfig.tryGetInt("port"), rundeckConfig.tryGetBoolean("ssl"), rundeckConfig.tryGetString("token")) {
      override val authToken: Option[String] = Some("my-super-secret-token")

      override protected val client: SingleNodeHttpClient = new SingleNodeHttpClient("rundeck", Duration.Top) {
        override def apply(request: Request, isIdempotent: Boolean = false): Future[ConsumedResponse] = {
          request.uri shouldEqual s"/api/16/job/perpetuo-deployment/executions?authtoken=my-super-secret-token"
          request.contentString shouldEqual """{"argString":"-callback-url 'http://somewhere/api/execution-traces/42' -product-name 'My\"Beautiful\"Project' -target 'a,b' -product-version \"the 042nd version\""}"""
          Future.value(ConsumedResponse(Status(statusMock), Utf8(contentMock), "rundeck"))
        }
      }
    }

    override protected val client: RundeckClient = new RundeckClientMock

    def testTrigger: Option[String] = {
      val productName = "My\"Beautiful\"Project"
      val version = Version(JsString("the 042nd version"))
      val target = TargetAtomSet(Set.empty, Set("a", "b").map(TargetAtom))
      Await.result(trigger(42, productName, version, target, "guy next door"), 1.second)
    }
  }

  test("Rundeck's API is followed when everything goes well") {
    new TriggerMock(200, """{"id": 123, "permalink": "http://rundeck/job/123/show"}""").testTrigger shouldEqual Some("http://rundeck/job/123/show")
  }

  test("Rundeck's API is followed when a connection problem occurs") {
    val exc = the[Exception] thrownBy new TriggerMock(403, "<html>gibberish</html>").testTrigger
    exc.getMessage shouldEqual "Bad response from Rundeck (on localhost) (job: perpetuo-deployment): Forbidden"
  }

  test("Rundeck's API is followed when an internal server error occurs") {
    val exc = the[Exception] thrownBy new TriggerMock(500, "<html><p>Intelligible error</p></html>").testTrigger
    exc.getMessage should endWith("Internal Server Error: Intelligible error")
  }

  test("Rundeck's API is followed when the request cannot be satisfied") {
    val exc = the[Exception] thrownBy new TriggerMock(400, """{"error": true, "message": "Intelligible error"}""").testTrigger
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
