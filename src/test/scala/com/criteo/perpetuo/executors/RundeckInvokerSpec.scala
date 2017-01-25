package com.criteo.perpetuo.executors

import com.criteo.perpetuo.dao.enums.Operation
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.inject.Test
import com.twitter.util.Future
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global


class RundeckInvokerSpec extends Test with Eventually {

  implicit val defaultPatience = PatienceConfig(timeout = Span(1, Seconds), interval = Span(100, Millis))

  private def testWhenResponseIs(statusCode: Int, content: String) = {
    object RundeckInvokerMock extends RundeckInvoker("HOST", 4242) {
      override lazy protected val authToken: String = "fake"

      override protected val client: (Request) => Future[Response] = request => {
        request.uri shouldEqual s"/api/$apiVersion/job/deploy/executions?authtoken=$authToken"
        request.contentString shouldEqual """{"argString":"-executionId 42 -productName \"MyBeautifulProject\" -version \"the last version\" -rawTarget \"{\\\"abc\\\": [\\\"def\\\", 42], \\\"ghi\\\": 51.3}\""}"""
        val resp = Response(Status(statusCode))
        resp.write(content)
        Future.value(resp)
      }
    }
    val uuid = RundeckInvokerMock.trigger(
      Operation.deploy,
      42,
      "MyBeautifulProject",
      "the last version",
      """{"abc": ["def", 42], "ghi": 51.3}""",
      "guy next door"
    )
    uuid shouldBe defined
    uuid.get
  }

  "Rundeck's API" should {
    "be followed" when {
      "everything goes well" in {
        eventually {
          testWhenResponseIs(200, """{"id": 123, "permalink": "http://rundeck/job/123/show"}""").map(
            _ shouldEqual "http://rundeck/job/123/show"
          )
        }
      }

      "a connection problem occurs" in {
        eventually {
          testWhenResponseIs(403, "<html>gibberish</html>").map(ans =>
            (the[Exception] thrownBy ans).getMessage shouldEqual "Rundeck answered: Forbidden"
          )
        }
      }

      "an internal server error occurs" in {
        eventually {
          testWhenResponseIs(500, "<html><p>Intelligible error</p></html>").map(ans =>
            (the[Exception] thrownBy ans).getMessage should endWith("Internal Server Error: Intelligible error")
          )
        }
      }

      "the request cannot be satisfied" in {
        eventually {
          testWhenResponseIs(400, """{"error": true, "message": "Intelligible error"}""").map(ans =>
            (the[Exception] thrownBy ans).getMessage should endWith("""Bad Request: "Intelligible error"""")
          )
        }
      }
    }
  }
}
