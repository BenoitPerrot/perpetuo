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


/** HTTP client for a basic usage of an HTTP API hosted on a single node */
class SingleNodeHttpClient(hostName: String, port: Option[Int], ssl: Option[Boolean],
                           connectionTimeout: Duration, requestTimeout: Duration, minimumDelayBetweenRetries: Duration,
                           retries: Int) extends Logging {

  def this(hostName: String, port: Option[Int], ssl: Option[Boolean], metronomePeriod: Duration, retries: Int = 5) =
    this(hostName, port, ssl, metronomePeriod, metronomePeriod, metronomePeriod, retries)

  def this(hostName: String, metronomePeriod: Duration) =
    this(hostName, None, None, metronomePeriod)

  private val https = ssl.getOrElse(port.contains(443))
  private val (protocol, dest) = {
    val (protocol, actualPort) = if (https) ("https", port.getOrElse(443)) else ("http", port.getOrElse(80))
    (protocol, s"$hostName:$actualPort")
  }

  def url(path: String): URL =
    port.map(new URL(protocol, hostName, _, path)).getOrElse(new URL(protocol, hostName, path))

  def createRequest(path: String, builder: RequestBuilder[Nothing, Nothing] = RequestBuilder()): RequestBuilder[RequestConfig.Yes, Nothing] =
    builder.url(url(path))

  private val service: Service[Request, Response] = {
    val shouldRetry: PartialFunction[(Request, Try[Response]), Boolean] = {
      case (request, Return(rep)) =>
        val code = rep.status.code
        if (500 <= code) {
          logger.warn(s"$hostName answered HTTP $code: ${rep.status.reason}")
          code == 503 || request.ctx(SingleNodeHttpClient.requestIdempotenceField)
        }
        else {
          if (400 <= code)
            logger.error(s"$hostName answered HTTP $code: ${rep.status.reason}")
          false
        }
      case (_, t@Throw(_: RequestException | _: ConnectionFailedException | _: WriteException | _: ServiceNotAvailableException)) =>
        logger.warn(s"Could not send a request to $hostName: ${t.throwable.getMessage}")
        true // failed before the request was sent
      case (request, _) =>
        request.ctx(SingleNodeHttpClient.requestIdempotenceField)
    }
    val backoff = Backoff.exponential(minimumDelayBetweenRetries, 2).take(retries)
    val retry = RetryFilter(backoff, DefaultStatsReceiver)(shouldRetry)(HighResTimer.Default)

    val sb = if (https) Http.client.withTls(hostName) else Http.client
    val baseService = sb
      .withSessionQualifier.noFailFast
      .withSessionQualifier.noFailureAccrual
      .withStreaming(true)
      .withSession.acquisitionTimeout(connectionTimeout)
      .withRequestTimeout(requestTimeout)
      .newService(dest, hostName)

    val loggingService = Service.mk { req: Request =>
      val start = System.currentTimeMillis()
      baseService(req).onSuccess(_ =>
        logger.debug(s"$hostName answered in ${System.currentTimeMillis() - start}ms")
      )
    }

    retry.andThen(loggingService)
  }

  /**
    * Send the request and return the response when its body has been fully received, possibly after retries.
    * Caution! Request instances must not be reused: provide a unique instance for each call to this method
    * or it will fail.
    */
  def apply(request: Request, isIdempotent: Boolean = false): Future[ConsumedResponse] = {
    request.ctx.updateAndLock(SingleNodeHttpClient.requestIdempotenceField, isIdempotent)
    val start = System.currentTimeMillis()
    service(request)
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


object SingleNodeHttpClient {
  val requestIdempotenceField: Request.Schema.Field[Boolean] = Request.Schema.newField()
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
