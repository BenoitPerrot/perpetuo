package com.criteo.perpetuo.util

import java.net.URL

import com.criteo.perpetuo.TestHelpers
import com.twitter.conversions.time._
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.http.{Request, RequestBuilder}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{Controller, EmbeddedHttpServer, HttpServer}
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration, Future}
import spray.json._


class SingleNodeHttpClientBuilderSpec extends TestHelpers {
  // timeouts: the highest the more deterministic the tests, the lowest the quicker
  private val connectionTimeout = 50.milliseconds
  private val requestTimeout = 100.milliseconds
  private val safePeriod = connectionTimeout + requestTimeout // needed for timeout-based assertions

  private val apiPath = "/api/resources"
  private val testController = new Controller {
    post(apiPath) { r: Request =>
      val body = r.contentString
      body.parseJson match {
        case _: JsObject => response.ok(body)
        case _ => response.badRequest
      }
    }
    get("/unavailable") { _: Request =>
      response.serviceUnavailable
    }
  }

  private val server = new EmbeddedHttpServer(new HttpServer {
    override def configureHttp(router: HttpRouter): Unit = router.add(testController)
  })
  private val Array(host, portStr) = server.externalHttpHostAndPort.split(":", 2)

  private val clientBuilder = new SingleNodeHttpClientBuilder(host, Some(portStr.toInt), Some(false))
  private val client = buildClient(clientBuilder)

  private val clientBuilderToUnknown = new SingleNodeHttpClientBuilder("unknown", Some(1234))
  private val rootUrl = RequestBuilder().url(url("/"))

  private def buildClient(builder: SingleNodeHttpClientBuilder) =
    builder.build(connectionTimeout, requestTimeout, safePeriod, 1)

  private def url(path: String) = new URL(s"http://$host:$portStr$path")

  private def post(body: String) = RequestBuilder().url(url("/api/resources")).buildPost(Buf.Utf8(body))

  private def shouldRetry(f: => Future[ConsumedResponse]) = {
    val start = System.currentTimeMillis
    val response = Await.result(f, safePeriod * 3)
    val elapsed = System.currentTimeMillis - start
    elapsed should be > safePeriod.inMilliseconds
    response.status.code
  }

  private def shouldNotRetry(f: => Future[ConsumedResponse]) = {
    Await.result(f, safePeriod).status.code
  }

  private def bootstrapResolution(clientBuilder: SingleNodeHttpClientBuilder, req: Request): Unit = {
    val client = clientBuilder.build(Duration.Top, 0)
    Await.ready(client(req))
  }

  // bootstrap the resolution to allow much shorter and more deterministic timeouts for following timeout-based assertions
  bootstrapResolution(clientBuilder, post("invalid"))
  bootstrapResolution(clientBuilderToUnknown, rootUrl.buildGet())

  test("Non-idempotent requests are not retried on an HTTP 500") {
    shouldNotRetry(client(post("invalid"))) shouldEqual 500
  }

  test("Idempotent requests are retried on an HTTP 500") {
    shouldRetry(client(post("invalid"), isIdempotent = true)) shouldEqual 500
  }

  test("Requests are always retried on an HTTP 503") {
    val rb = RequestBuilder().url(url("/unavailable"))
    shouldRetry(client(rb.buildGet())) shouldEqual 503
    shouldRetry(client(rb.buildGet(), isIdempotent = true)) shouldEqual 503
  }

  test("Requests are never retried on an HTTP 400") {
    shouldNotRetry(client(post("[]"))) shouldEqual 400
    shouldNotRetry(client(post("[]"), isIdempotent = true)) shouldEqual 400
  }

  test("Requests are never retried on an HTTP 200") {
    shouldNotRetry(client(post("{}"))) shouldEqual 200
    shouldNotRetry(client(post("{}"), isIdempotent = true)) shouldEqual 200
  }

  test("Requests are retried if the service is unknown") {
    val client = buildClient(clientBuilderToUnknown)
    a[NoBrokersAvailableException] shouldBe thrownBy(shouldRetry(client(rootUrl.buildGet())))
  }

  test("SingleNodeHttpClient fails if a Request instance is reused") {
    // we must check the request cannot be reused because the current implementation makes the instance stateful
    val builder = new SingleNodeHttpClientBuilder("host")
    val client = builder.build(1.second)
    val req = builder.createRequest("/path").buildGet()
    client(req)
    a[Throwable] shouldBe thrownBy(client(req))
  }
}
