package com.criteo.perpetuo.engine.executors

import java.net.{InetSocketAddress, URL, URLEncoder}

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.twitter.conversions.time._
import com.twitter.finagle.Http.Client
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Request, RequestBuilder, Response}
import com.twitter.finagle.http.Status.{NotFound, Ok, Found}
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.inject.Logging
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future, Try}


class JenkinsClient(val host: String) extends Logging {
  val config = AppConfigProvider.executorConfig("jenkins")
  val port: Int = config.getIntOrElse("port", 80)

  // Username and Apitoken of the Jenkins user used to authenticate to Jenkins server
  val username: Option[String] = config.tryGetString("username")
  val password: Option[String] = config.tryGetString("password")
  private val userInfoPrefix: String =
    (username, password) match {
      case (Some(u), Some(p)) => s"$u:$p@"
      case (None, None) => ""
      case _ =>
        logger.warn(s"Incomplete authentication setup: only one of username and password was provided, while both or none are expected")
        ""
    }

  // Timeouts
  private val requestTimeout: Duration = 5.seconds

  def maxAbortDuration: Duration = requestTimeout

  // HTTP client
  private val ssl: Boolean = port == 443
  private val protocol: String = if (ssl) "https" else "http"
  private val maxConnectionsPerHost: Int = 10
  private val backoffDurations: Stream[Duration] = Backoff.exponentialJittered(1.seconds, 5.seconds)
  private val backoffPolicy: RetryPolicy[Try[Nothing]] = RetryPolicy.backoff(backoffDurations)(RetryPolicy.TimeoutAndWriteExceptionsOnly)

  private def post(apiSubPath: String): Future[Response] =
    client(
      RequestBuilder()
        .url(new URL(protocol, host, userInfoPrefix + apiSubPath))
        .buildPost(Buf.Empty)
    )

  private val client: Request => Future[Response] = (if (ssl) ClientBuilder().tlsWithoutValidation else ClientBuilder())
    .stack(Client())
    .timeout(requestTimeout)
    .hostConnectionLimit(maxConnectionsPerHost)
    .hosts(new InetSocketAddress(host, port))
    .retryPolicy(backoffPolicy)
    .failFast(false)
    .build()

  private[executors] def apiPath(apiSubPath: String, queryParameters: Map[String, String] = Map()): String = {
    if (queryParameters.nonEmpty) {
      val queryString = queryParameters.map { case (k, v) =>
        val encodedVal = URLEncoder.encode(v, "UTF-8")
        val encodedKey = URLEncoder.encode(k, "UTF-8")
        s"$encodedKey=$encodedVal"
      }.mkString("&")

      s"/$apiSubPath?$queryString"
    } else
      s"/$apiSubPath"
  }

  def abortJob(jobName: String, jobId: String): Future[JenkinsJobState.ExecState] =
    post(apiPath(s"job/$jobName/$jobId/stop")).flatMap(resp =>
      resp.status match {
        case NotFound =>
          Future.value(JenkinsJobState.notFound)
        case Ok | Found =>
          Future.value(JenkinsJobState.terminated)
        case error =>
          // todo: return a status and a reason from the stopper
          Future.exception(new RuntimeException(s"Jenkins error (${error.code}): ${error.reason}"))
      }
    )

  def startJob(jobName: String, jobToken: Option[String], parameters: Map[String, String] = Map()): Future[Response] = {
    post(apiPath(s"job/$jobName/buildWithParameters", jobToken.map(t => Map("token" -> t)).getOrElse(Map()) ++ parameters))
  }
}


object JenkinsJobState extends Enumeration {
  type ExecState = Value

  val terminated: ExecState = Value("terminated")
  val notFound: ExecState = Value("notFound")
}
