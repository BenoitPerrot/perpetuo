package com.criteo.perpetuo.engine.executors

import java.net.InetSocketAddress

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.twitter.conversions.time._
import com.twitter.finagle.Http.Client
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Status.{NotFound, Ok}
import com.twitter.finagle.http.{Message, Method, Request, Response}
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.util.{Await, Duration, Future => TwitterFuture, Try => TwitterTry}
import spray.json.DefaultJsonProtocol._
import spray.json._


class RundeckClient(val host: String) {
  val apiVersion = 16

  private val config = AppConfigProvider.executorConfig("rundeck")
  val port: Int = config.getIntOrElse("port", 80)
  val authToken: Option[String] = config.tryGetString("token")

  // Timeout
  val waitInterval: Duration = 100.milliseconds
  val jobTerminationTimeout: Duration = 2.seconds
  val baseTimeout: Duration = 1.second

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

  private def isJobCompleted(parsedContent: JsObject): Boolean =
    parsedContent.fields("execCompleted").asInstanceOf[JsBoolean].value

  private def isAbortFailed(parsedContent: JsObject): Boolean =
    parsedContent.fields("abort").asJsObject.fields("status").asInstanceOf[JsString].value == "failed"

  private def isJobRunning(parsedContent: JsObject): Boolean =
    parsedContent.fields("execution").asJsObject.fields("status").asInstanceOf[JsString].value == "running"

  def abortJob(jobId: String): TwitterFuture[RundeckJobState.ExecState] =
    fetch(apiPath(s"execution/$jobId/abort")).map(resp =>
      resp.status match {
        case NotFound =>
          RundeckJobState.notFound
        case Ok =>
          val body = resp.contentString.parseJson.asJsObject
          body match {
            case s if !isJobRunning(s) => RundeckJobState.terminated
            case s if isAbortFailed(s) => throw new RuntimeException(body.fields("abort").asJsObject.fields("reason").asInstanceOf[JsString].value)
            case _ => fetchJobFinalState(jobId)
          }
        case error =>
          // todo: return a status and a reason from the stopper
          throw new RuntimeException(s"Rundeck error (${error.code}): ${error.reason}")
      }
    )

  def fetchJobState(jobId: String): TwitterFuture[RundeckJobState.ExecState] =
    fetch(apiPath(s"execution/$jobId/output/state?stateOnly=true")).map(resp =>
      resp.status match {
        case NotFound => RundeckJobState.notFound
        case Ok if isJobCompleted(resp.contentString.parseJson.asJsObject) => RundeckJobState.terminated
        case Ok => RundeckJobState.running
        case error => throw new RuntimeException(s"Rundeck error (${error.code}): ${error.reason}")
        // todo: return a status and a reason from the stopper
      }
    )

  private def fetchJobFinalState(jobId: String): RundeckJobState.ExecState = {
    var totalWaited: Duration = 0.millisecond
    var incrementalWaitInterval: Duration = waitInterval

    var execState = RundeckJobState.running
    while (execState == RundeckJobState.running && totalWaited < jobTerminationTimeout) {
      Thread.sleep(incrementalWaitInterval.inMilliseconds)
      execState = Await.result(fetchJobState(jobId), baseTimeout)
      totalWaited += incrementalWaitInterval
      incrementalWaitInterval += waitInterval
    }
    execState
  }
}


object RundeckJobState extends Enumeration {
  type ExecState = Value

  val running = Value("running")
  val terminated = Value("terminated")
  val notFound = Value("notFound")
}
