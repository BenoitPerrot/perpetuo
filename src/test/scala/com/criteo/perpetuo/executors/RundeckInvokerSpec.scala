package com.criteo.perpetuo.executors

import com.criteo.perpetuo.model.Operation
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.inject.Test
import com.twitter.util.Future

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class RundeckInvokerSpec extends Test {

  private def testWhenResponseIs(statusCode: Int, content: String) = {
    object RundeckInvokerMock extends RundeckInvoker("host-example", 4242, "rundeck") {
      override protected val client: (Request) => Future[Response] = request => {
        request.uri shouldEqual s"/api/$apiVersion/job/deploy/executions?authtoken=$authToken"
        request.contentString shouldEqual """{"argString":"-executionId 42 -productName \"MyBeautifulProject\" -version \"the last version\" -rawTarget \"{\\\"abc\\\": [\\\"def\\\", 42], \\\"ghi\\\": 51.3}\""}"""
        val resp = Response(Status(statusCode))
        resp.write(content)
        Future.value(resp)
      }
    }
    val logHref = RundeckInvokerMock.trigger(
      Operation.deploy,
      42,
      "MyBeautifulProject",
      "the last version",
      """{"abc": ["def", 42], "ghi": 51.3}""",
      "guy next door"
    )
    logHref shouldBe defined
    logHref.get
  }

  "Rundeck's API" should {
    "be followed" when {
      "everything goes well" in {
        Await.result(
          testWhenResponseIs(200, """{"id": 123, "permalink": "http://rundeck/job/123/show"}""").map(
            _ shouldEqual "http://rundeck/job/123/show"
          ),
          1.second
        )
      }

      "a connection problem occurs" in {
        Await.result(
          testWhenResponseIs(403, "<html>gibberish</html>").recover {
            case err: Exception => err.getMessage shouldEqual "Rundeck answered: Forbidden"
          },
          1.second
        )
      }

      "an internal server error occurs" in {
        Await.result(
          testWhenResponseIs(500, "<html><p>Intelligible error</p></html>").recover {
            case err: Exception => err.getMessage should endWith("Internal Server Error: Intelligible error")
          },
          1.second
        )
      }

      "the request cannot be satisfied" in {
        Await.result(
          testWhenResponseIs(400, """{"error": true, "message": "Intelligible error"}""").recover {
            case err: Exception => err.getMessage should endWith("""Bad Request: "Intelligible error"""")
          },
          1.second
        )
      }
    }
  }
}
