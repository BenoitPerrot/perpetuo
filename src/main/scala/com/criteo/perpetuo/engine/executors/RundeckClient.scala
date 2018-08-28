package com.criteo.perpetuo.engine.executors

import java.net.{InetSocketAddress, URL}

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.twitter.conversions.time._
import com.twitter.finagle.Http.Client
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.Status.{NotFound, Ok}
import com.twitter.finagle.http._
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.finatra.http.HttpHeaders
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future, Try}
import spray.json._

class RundeckClient(val host: String) {
  val apiVersion = 16

  private val config = AppConfigProvider.executorConfig("rundeck")
  val port: Int = config.getIntOrElse("port", 80)
  val authToken: Option[String] = config.tryGetString("token")

  // Timeouts
  private val requestTimeout: Duration = 5.seconds
  protected val baseWaitInterval: Duration = 100.milliseconds
  protected val jobTerminationTimeout: Duration = 5.seconds

  def maxAbortDuration: Duration = requestTimeout * 2 + jobTerminationTimeout

  // HTTP client
  private val ssl: Boolean = port == 443
  private val protocol: String = if (ssl) "https" else "http"
  private val maxConnectionsPerHost: Int = 10
  private val backoffDurations: Stream[Duration] = Backoff.exponentialJittered(1.seconds, 5.seconds).take(5)
  private val backoffPolicy: RetryPolicy[Try[Nothing]] = RetryPolicy.backoff(backoffDurations)(RetryPolicy.TimeoutAndWriteExceptionsOnly)

  private val jsonRequestBuilder = RequestBuilder()
    .setHeader(HttpHeaders.ContentType, Message.ContentTypeJson)
    .setHeader(HttpHeaders.Accept, Message.ContentTypeJson)

  private def post(apiSubPath: String, body: Option[JsValue] = None): Future[Response] =
    client(
      jsonRequestBuilder
        .url(new URL(protocol, host, apiSubPath))
        .buildPost(body.map(_.compactPrint).map(Buf.Utf8(_)).getOrElse(Buf.Empty))
    )

  protected val client: Request => Future[Response] = (if (ssl) ClientBuilder().tlsWithoutValidation else ClientBuilder())
    .stack(Client())
    .name(getClass.getSimpleName)
    .timeout(requestTimeout)
    .hostConnectionLimit(maxConnectionsPerHost)
    .hosts(new InetSocketAddress(host, port))
    .retryPolicy(backoffPolicy)
    .failFast(false)
    .build()

  private def apiPath(apiSubPath: String, queryParameters: Map[String, String] = Map()): String = {
    val path = s"/api/$apiVersion/$apiSubPath"
    val q = authToken.map(t => Map("authtoken" -> t)).getOrElse(Map()) ++ queryParameters
    if (q.nonEmpty) s"$path?${q.map { case (k, v) => s"$k=$v" }.mkString("&")}" else path
  }

  def startJob(jobName: String, parameters: Map[String, String] = Map()): Future[Response] = {
    // Rundeck before API version 18 does not support invocation with structured arguments
    val body = JsObject(
      "argString" -> JsString(parameters.toStream
          .map { case (parameterName, value) =>
            s"-$parameterName $value"
          }
          .mkString(" "))
    )

    post(apiPath(s"job/$jobName/executions"), Some(body))
  }

  private def isJobCompleted(parsedContent: JsObject): Boolean =
    parsedContent.fields("execCompleted").asInstanceOf[JsBoolean].value

  private def isAbortFailed(parsedContent: JsObject): Boolean =
    parsedContent.fields("abort").asJsObject.fields("status").asInstanceOf[JsString].value == "failed"

  private def isJobRunning(parsedContent: JsObject): Boolean =
    parsedContent.fields("execution").asJsObject.fields("status").asInstanceOf[JsString].value == "running"

  def abortJob(jobId: String): Future[RundeckJobState.ExecState] =
    post(apiPath(s"execution/$jobId/abort")).flatMap(resp =>
      resp.status match {
        case NotFound =>
          Future.value(RundeckJobState.notFound)
        case Ok =>
          val body = resp.contentString.parseJson.asJsObject
          body match {
            case s if !isJobRunning(s) => Future.value(RundeckJobState.terminated)
            case s if isAbortFailed(s) => Future.exception(new RuntimeException(body.fields("abort").asJsObject.fields("reason").asInstanceOf[JsString].value))
            case _ => waitForJobFinalState(jobId)
          }
        case error =>
          // todo: return a status and a reason from the stopper
          Future.exception(new RuntimeException(s"Rundeck error (${error.code}): ${error.reason}"))
      }
    )

  def fetchJobState(jobId: String): Future[RundeckJobState.ExecState] =
    post(apiPath(s"execution/$jobId/output/state", Map("stateOnly" -> "true"))).map(resp =>
      resp.status match {
        case NotFound => RundeckJobState.notFound
        case Ok if isJobCompleted(resp.contentString.parseJson.asJsObject) => RundeckJobState.terminated
        case Ok => RundeckJobState.running
        case error => throw new RuntimeException(s"Rundeck error (${error.code}): ${error.reason}")
        // todo: return a status and a reason from the stopper
      }
    )

  private def waitForJobFinalState(jobId: String): Future[RundeckJobState.ExecState] = {
    val deadlineInNs = System.nanoTime() + jobTerminationTimeout.inNanoseconds

    def loopWhileRunning(sleepTime: Duration): Future[RundeckJobState.ExecState] = {
      if (System.nanoTime() < deadlineInNs) {
        Thread.sleep(sleepTime.inMilliseconds)
        fetchJobState(jobId).flatMap(status =>
          if (status == RundeckJobState.running)
            loopWhileRunning(sleepTime + baseWaitInterval)
          else
            Future.value(status)
        )
      }
      else
        Future.value(RundeckJobState.running)
    }

    loopWhileRunning(baseWaitInterval)
  }
}


object RundeckJobState extends Enumeration {
  type ExecState = Value

  val running: ExecState = Value("running")
  val terminated: ExecState = Value("terminated")
  val notFound: ExecState = Value("notFound")
}
