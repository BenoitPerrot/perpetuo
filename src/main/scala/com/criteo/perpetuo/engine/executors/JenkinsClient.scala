package com.criteo.perpetuo.engine.executors

import java.net.URLEncoder

import com.criteo.perpetuo.util.{ConsumedResponse, SingleNodeHttpClient}
import com.twitter.conversions.time._
import com.twitter.finagle.http.Status.{Found, NotFound, Ok}
import com.twitter.inject.Logging
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future}


class JenkinsClient(val host: String, port: Option[Int], ssl: Option[Boolean], username: Option[String], password: Option[String]) extends Logging {
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

  private val client = new SingleNodeHttpClient(host, port, ssl, requestTimeout)

  private def post(apiSubPath: String): Future[ConsumedResponse] =
    client(client.createRequest(userInfoPrefix + apiSubPath).buildPost(Buf.Empty))

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

  def startJob(jobName: String, jobToken: Option[String], parameters: Map[String, String] = Map()): Future[ConsumedResponse] = {
    post(apiPath(s"job/$jobName/buildWithParameters", jobToken.map(t => Map("token" -> t)).getOrElse(Map()) ++ parameters))
      .map(_.raiseForStatus)
  }
}


object JenkinsJobState extends Enumeration {
  type ExecState = Value

  val terminated: ExecState = Value("terminated")
  val notFound: ExecState = Value("notFound")
}
