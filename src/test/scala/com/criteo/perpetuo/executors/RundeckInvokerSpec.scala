package com.criteo.perpetuo.executors

import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dispatchers.{TargetDispatcher, TargetTerm}
import com.criteo.perpetuo.model.{Operation, Version}
import com.twitter.finagle.http.{Response, Status}
import com.twitter.inject.Test
import com.twitter.util.Future
import com.typesafe.config.ConfigValueFactory
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class RundeckInvokerSpec extends Test {
  private def testWhenResponseIs(statusCode: Int, content: String) = {
    val testEnv = ConfigValueFactory.fromAnyRef("local")
    val plugins = new Plugins(AppConfig.withValue("env", testEnv))
    val dispatcher = plugins.instantiateFromGroovy[TargetDispatcher](AppConfig.get("plugins.dispatcher"))
    val rundeckInvoker = dispatcher.assign("foo").head.asInstanceOf[HttpInvoker]
    assert(rundeckInvoker.getClass.getSimpleName == "RundeckInvoker")
    rundeckInvoker.client = request => {
      request.uri shouldEqual s"/api/16/job/deploy-to-marathon/executions?authtoken=my-super-secret-token"
      request.contentString shouldEqual """{"argString":"-environment preprod -callback-url 'http://somewhere/api/execution-traces/42' -product-name \"My\\\"Beautiful\\\"Project\" -product-version \"the 42nd version\" -target \"a,b\""}"""
      val resp = Response(Status(statusCode))
      resp.write(content)
      Future.value(resp)
    }
    val logHref = rundeckInvoker.trigger(
      Operation.deploy.toString,
      42,
      "My\"Beautiful\"Project",
      Version("the 042nd version"),
      Set(TargetTerm(Set(JsObject("abc" -> JsString("def"), "ghi" -> JsNumber(51.3))), Set("a", "b"))),
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
            case err: Exception => err.getMessage should endWith("""Bad Request: Intelligible error""")
          },
          1.second
        )
      }
    }
  }
}
