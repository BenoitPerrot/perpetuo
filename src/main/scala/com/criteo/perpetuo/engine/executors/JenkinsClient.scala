package com.criteo.perpetuo.engine.executors

import java.net.{InetSocketAddress, URLEncoder}

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.twitter.conversions.time._
import com.twitter.finagle.Http.Client
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Method, Request, Response}
import com.twitter.finagle.http.Status.{NotFound, Ok, Found}
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.inject.Logging
import com.twitter.util.{Base64StringEncoder, Duration, Future => TwitterFuture, Try => TwitterTry}


class JenkinsClient(val host: String) extends Logging {
  val config = AppConfigProvider.executorConfig("jenkins")
  val port: Int = config.getIntOrElse("port", 80)

  // Username and Apitoken of the Jenkins user used to authenticate to Jenkins server
  val username: Option[String] = config.tryGetString("username")
  val password: Option[String] = config.tryGetString("password")

  // Timeouts
  protected val requestTimeout: Duration = 5.seconds

  def maxAbortDuration: Duration = requestTimeout

  // HTTP client
  protected def ssl: Boolean = port == 443

  protected val maxConnectionsPerHost: Int = 10
  protected val backoffDurations: Stream[Duration] = Backoff.exponentialJittered(1.seconds, 5.seconds)
  protected val backoffPolicy: RetryPolicy[TwitterTry[Nothing]] = RetryPolicy.backoff(backoffDurations)(RetryPolicy.TimeoutAndWriteExceptionsOnly)

  protected def post(apiSubPath: String): TwitterFuture[Response] =
    client(buildPostRequest(apiSubPath))

  def createBasicAuthenticationHeader(username: Option[String], password: Option[String]): Option[String] =
    (username, password) match {
      case (Some(u), Some(p)) => Some(s"Basic " + Base64StringEncoder.encode(s"$u:$p".getBytes("UTF-8")))
      case (None, None) => None
      case _ => {
        logger.warn(s"Jenkins Client ${host}: Username or Password provided but not both")
        None
      }
    }

  protected lazy val client: Request => TwitterFuture[Response] = (if (ssl) ClientBuilder().tlsWithoutValidation else ClientBuilder())
    .stack(Client())
    .timeout(requestTimeout)
    .hostConnectionLimit(maxConnectionsPerHost)
    .hosts(new InetSocketAddress(host, port))
    .retryPolicy(backoffPolicy)
    .failFast(false)
    .build()

  private[executors] def apiPath(apiSubPath: String, jobToken: Option[String] = None, queryParameters: Map[String, String] = Map()): String = {
    val q = jobToken.map(t => Map("token" -> t)).getOrElse(Map()) ++ queryParameters
    if (q.nonEmpty) {
      val queryString = q.map { case (k, v) =>
        val encodedVal = URLEncoder.encode(v, "UTF-8")
        val encodedKey = URLEncoder.encode(k, "UTF-8")
        s"$encodedKey=$encodedVal"
      }.mkString("&")

      s"/$apiSubPath?$queryString"
    } else
      s"/$apiSubPath"
  }

  protected def buildPostRequest(apiSubPath: String): Request = {

    val req = Request(Method.Post, apiSubPath)
    req.host = host
    createBasicAuthenticationHeader(username, password).foreach(req.authorization = _)
    req
  }

  def abortJob(jobName: String, jobId: String): TwitterFuture[JenkinsJobState.ExecState] =
    post(apiPath(s"job/$jobName/$jobId/stop")).flatMap(resp =>
      resp.status match {
        case NotFound =>
          TwitterFuture(JenkinsJobState.notFound)
        case Ok | Found =>
          TwitterFuture(JenkinsJobState.terminated)
        case error =>
          // todo: return a status and a reason from the stopper
          throw new RuntimeException(s"Jenkins error (${error.code}): ${error.reason}")
      }
    )

  def startJob(jobName: String, jobToken: Option[String], parameters: Map[String, String] = Map()): TwitterFuture[Response] = {
    post(apiPath(s"job/$jobName/buildWithParameters", jobToken, parameters))
  }
}


object JenkinsJobState extends Enumeration {
  type ExecState = Value

  val terminated: ExecState = Value("terminated")
  val notFound: ExecState = Value("notFound")
}
