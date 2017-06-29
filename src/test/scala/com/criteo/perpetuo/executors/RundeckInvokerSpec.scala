package com.criteo.perpetuo.executors

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.config.{AppConfig, Plugins}
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.dispatchers.TargetTerm
import com.criteo.perpetuo.model.{Operation, Version}
import com.twitter.finagle.http.{Response, Status}
import com.twitter.inject.Test
import com.twitter.util.Future
import com.typesafe.config.ConfigValueFactory
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration._


class RundeckInvokerSpec extends Test with TestDb {
  private def testWhenResponseIs(statusCode: Int, content: String) = {
    val testEnv = ConfigValueFactory.fromAnyRef("local")
    val plugins = new Plugins(new DbBinding(dbContext), AppConfig.withValue("env", testEnv))
    val rundeckInvoker = plugins.dispatcher.dispatchToExecutors("foo").head.asInstanceOf[HttpInvoker]
    assert(rundeckInvoker.getClass.getSimpleName == "RundeckInvoker")
    rundeckInvoker.client = request => {
      request.uri shouldEqual s"/api/16/job/deploy-to-marathon/executions?authtoken=my-super-secret-token"
      request.contentString shouldEqual """{"argString":"-callback-url 'http://somewhere/api/execution-traces/42' -product-name \"My\\\"Beautiful\\\"Project\" -target \"a,b\" -product-version \"the 42nd version\""}"""
      val resp = Response(Status(statusCode))
      resp.write(content)
      Future.value(resp)
    }
    val parameters = rundeckInvoker.freezeParameters(Operation.executionKind(Operation.deploy), "My\"Beautiful\"Project", Version("the 042nd version").compactPrint)
    val logHref = Await.result(rundeckInvoker.trigger(42, Set(TargetTerm(Set(JsObject("abc" -> JsString("def"), "ghi" -> JsNumber(51.3))), Set("a", "b"))), parameters, "guy next door"), 1.second)
    logHref shouldBe defined
    logHref.get
  }

  "Rundeck's API" should {
    "be followed" when {
      "everything goes well" in {
        testWhenResponseIs(200, """{"id": 123, "permalink": "http://rundeck/job/123/show"}""") shouldEqual "http://rundeck/job/123/show"
      }

      "a connection problem occurs" in {
        val exc = the[Exception] thrownBy testWhenResponseIs(403, "<html>gibberish</html>")
        exc.getMessage shouldEqual "Rundeck answered: Forbidden"
      }

      "an internal server error occurs" in {
        val exc = the[Exception] thrownBy testWhenResponseIs(500, "<html><p>Intelligible error</p></html>")
        exc.getMessage should endWith("Internal Server Error: Intelligible error")
      }

      "the request cannot be satisfied" in {
        val exc = the[Exception] thrownBy testWhenResponseIs(400, """{"error": true, "message": "Intelligible error"}""")
        exc.getMessage should endWith("""Bad Request: Intelligible error""")
      }
    }
  }
}
