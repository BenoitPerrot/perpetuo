package com.criteo.perpetuo.util

import java.net.URL

import com.twitter.finagle._
import com.twitter.finagle.http.{Request, RequestBuilder, RequestConfig, Response}
import com.twitter.finagle.param.HighResTimer
import com.twitter.finagle.service._
import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.inject.Logging
import com.twitter.util._


/** Build an HTTP client for a basic usage of an HTTP API hosted on a single node */
class SingleNodeHttpClientBuilder(hostName: String, port: Option[Int] = None, ssl: Option[Boolean] = None) extends Logging {
  private val resolvedSsl = ssl.getOrElse(port.contains(443))
  private val resolvedPort = port.getOrElse(if (ssl.contains(true)) 443 else 80)

  private val protocol = if (resolvedSsl) "https" else "http"
  private val dest = s"$hostName:$resolvedPort"
  private val serviceBuilder: Http.Client = (if (resolvedSsl) Http.client.withTls(hostName) else Http.client)
    .withSessionQualifier.noFailFast
    .withSessionQualifier.noFailureAccrual

  def createRequest(path: String, builder: RequestBuilder[Nothing, Nothing] = RequestBuilder()): RequestBuilder[RequestConfig.Yes, Nothing] =
    builder.url(new URL(protocol, hostName, resolvedPort, path))

  def build(connectionTimeout: Duration, requestTimeout: Duration, minimumDelayBetweenRetries: Duration, retries: Int, areRequestsIdempotent: Boolean): Request => Future[Response] = {
    val shouldRetry: PartialFunction[(Request, Try[Response]), Boolean] = {
      case (_, Return(rep)) =>
        val code = rep.status.code
        if (500 <= code)
          logger.warn(s"$hostName answered HTTP $code: ${rep.status.reason}")
        else if (400 <= code)
          logger.error(s"$hostName answered HTTP $code: ${rep.status.reason}")
        rep.status.code == 503 || (areRequestsIdempotent && 500 <= rep.status.code)
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
    r: Request => { // logging client
      val start = System.currentTimeMillis()
      client(r).respond(_ => logger.debug(s"Request to $hostName (including possible retries) took ${System.currentTimeMillis() - start}ms"))
    }
  }

  def build(metronomePeriod: Duration, retries: Int = 5, areRequestsIdempotent: Boolean = false): Request => Future[Response] =
    build(metronomePeriod, metronomePeriod, metronomePeriod, retries, areRequestsIdempotent)
}
