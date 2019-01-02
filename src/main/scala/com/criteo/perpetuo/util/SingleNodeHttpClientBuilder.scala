package com.criteo.perpetuo.util

import java.net.URL
import java.nio.charset.StandardCharsets

import com.twitter.finagle._
import com.twitter.finagle.http.{Request, RequestBuilder, RequestConfig, Response}
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service._
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.inject.Logging
import com.twitter.io.{Buf, Reader}
import com.twitter.util._
import spray.json._


/** Build an HTTP client for a basic usage of an HTTP API hosted on a single node */
class SingleNodeHttpClientBuilder(hostName: String, port: Option[Int] = None, security: Option[TransportSecurity.Value] = None) extends Logging {
  private val actualSecurity = security.getOrElse(if (port.contains(443)) TransportSecurity.Ssl else TransportSecurity.NoSsl)
  private val (protocol, dest) = {
    val (protocol, actualPort) = if (actualSecurity == TransportSecurity.NoSsl)
      ("http", port.getOrElse(80))
    else
      ("https", port.getOrElse(443))
    (protocol, s"$hostName:$actualPort")
  }

  private val serviceBuilder: Http.Client = {
    val sb = actualSecurity match {
      case TransportSecurity.NoSsl => Http.client
      case TransportSecurity.Ssl => Http.client.withTls(hostName)
    }
    sb.withSessionQualifier.noFailFast
      .withSessionQualifier.noFailureAccrual
      .withStreaming(true)
  }

  def url(path: String): URL =
    port.map(new URL(protocol, hostName, _, path)).getOrElse(new URL(protocol, hostName, path))

  def createRequest(path: String, builder: RequestBuilder[Nothing, Nothing] = RequestBuilder()): RequestBuilder[RequestConfig.Yes, Nothing] =
    builder.url(url(path))

  def build(connectionTimeout: Duration, requestTimeout: Duration, minimumDelayBetweenRetries: Duration, retries: Int, areRequestsIdempotent: Boolean): Request => Future[ConsumedResponse] = {
    val shouldRetry: PartialFunction[(Request, Try[Response]), Boolean] = {
      case (_, Return(rep)) =>
        val code = rep.status.code
        if (500 <= code) {
          logger.warn(s"$hostName answered HTTP $code: ${rep.status.reason}")
          code == 503 || areRequestsIdempotent
        }
        else {
          if (400 <= code)
            logger.error(s"$hostName answered HTTP $code: ${rep.status.reason}")
          false
        }
      case (_, t@Throw(_: RequestException | _: ConnectionFailedException | _: WriteException | _: ServiceNotAvailableException)) =>
        logger.warn(s"Could not send a request to $hostName: ${t.throwable.getMessage}")
        true // failed before the request was sent
    }
    val backoff = Backoff.exponential(minimumDelayBetweenRetries, 2).take(retries)
    val retry = RetryFilter(backoff, DefaultStatsReceiver)(shouldRetry)(HighResTimer.Default)

    val service = serviceBuilder
      .withSession.acquisitionTimeout(connectionTimeout)
      .withRequestTimeout(requestTimeout)
      .newService(dest, hostName)
    val loggingService = Service.mk { req: Request =>
      val start = System.currentTimeMillis()
      service(req).onSuccess(_ => logger.debug(s"$hostName answered in ${System.currentTimeMillis() - start}ms"))
    }

    val client = retry.andThen(loggingService)
    req: Request => {
      val start = System.currentTimeMillis()
      client(req)
        .flatMap { resp =>
          Reader.readAll(resp.reader).map { buf =>
            resp.reader.discard()
            logger.debug(s"Took ${System.currentTimeMillis() - start}ms total for $hostName to respond HTTP ${resp.status.code}")
            ConsumedResponse(resp.status, buf, hostName)
          }
        }
        .onFailure(err =>
          logger.debug(s"Request to $hostName failed after ${System.currentTimeMillis() - start}ms (including possible retries): ${err.getMessage}")
        )
    }
  }

  def build(metronomePeriod: Duration, retries: Int = 5, areRequestsIdempotent: Boolean = false): Request => Future[ConsumedResponse] =
    build(metronomePeriod, metronomePeriod, metronomePeriod, retries, areRequestsIdempotent)
}


object TransportSecurity extends Enumeration {
  val NoSsl: Value = Value
  val Ssl: Value = Value
}


case class ConsumedResponse(status: http.Status, content: Buf, serviceName: String) {
  lazy val contentString: String = Buf.decodeString(content, StandardCharsets.UTF_8)

  lazy val contentJson: JsValue = contentString.parseJson

  def raiseForStatus(contentInError: Boolean = false): ConsumedResponse = status match {
    case http.Status.Successful(_) =>
      this
    case _ =>
      val msg = s"Request to $serviceName failed with status HTTP ${status.code}"
      throw new RuntimeException(if (contentInError) s"$msg: $contentString" else msg)
  }
}
