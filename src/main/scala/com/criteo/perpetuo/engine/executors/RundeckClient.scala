package com.criteo.perpetuo.engine.executors

import java.net.InetSocketAddress

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.twitter.conversions.time._
import com.twitter.finagle.Http.Client
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Message, Method, Request, Response}
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.util.{Duration, Future => TwitterFuture, Try => TwitterTry}
import spray.json.DefaultJsonProtocol._
import spray.json._


class RundeckClient(val host: String) {
  val apiVersion = 16

  private val config = AppConfigProvider.executorConfig("rundeck")
  val port: Int = config.getIntOrElse("port", 80)
  val authToken: Option[String] = config.tryGetString("token")

  // HTTP client
  protected def ssl: Boolean = port == 443
  protected val requestTimeout: Duration = 20.seconds
  protected val maxConnectionsPerHost: Int = 10
  protected val backoffDurations: Stream[Duration] = Backoff.exponentialJittered(1.seconds, 5.seconds)
  protected val backoffPolicy: RetryPolicy[TwitterTry[Nothing]] = RetryPolicy.backoff(backoffDurations)(RetryPolicy.TimeoutAndWriteExceptionsOnly)

  protected def fetch(apiSubPath: String, parameters: Map[String, String] = Map()): TwitterFuture[Response] =
    client(buildRequest(apiSubPath, parameters))

  protected lazy val client: Request => TwitterFuture[Response] = (if (ssl) ClientBuilder().tlsWithoutValidation else ClientBuilder())
    .stack(Client())
    .timeout(requestTimeout)
    .hostConnectionLimit(maxConnectionsPerHost)
    .hosts(new InetSocketAddress(host, port))
    .retryPolicy(backoffPolicy)
    .failFast(false)
    .build()

  protected def apiPath(apiSubPath: String): String = {
    val path = s"/api/$apiVersion/$apiSubPath"
    authToken.map(t => s"$path?authtoken=$t").getOrElse(path)
  }

  protected def buildRequest(apiSubPath: String, parameters: Map[String, String] = Map()): Request = {

    // Rundeck before API version 18 does not support invocation with structured arguments
    val argString = parameters.toStream
      .map { case (parameterName, value) =>
        s"-$parameterName $value"
      }
      .mkString(" ")

    val body = Map("argString" -> argString).toJson

    val req = Request(Method.Post, apiSubPath)
    req.host = host
    req.contentType = Message.ContentTypeJson
    req.accept = Message.ContentTypeJson // default response format is XML
    req.contentString = body.compactPrint
    req
  }

  def startJob(jobName: String, parameters: Map[String, String] = Map()): TwitterFuture[Response] =
    fetch(apiPath(s"job/$jobName/executions"), parameters)
}
