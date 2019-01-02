package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.util.{ConsumedResponse, SingleNodeHttpClientBuilder, TransportSecurity}
import com.twitter.conversions.time._
import com.twitter.finagle.http.Status.{NotFound, Ok}
import com.twitter.finagle.http._
import com.twitter.finatra.http.HttpHeaders
import com.twitter.io.Buf
import com.twitter.util.{Duration, Future}
import com.typesafe.config.Config
import spray.json._

class RundeckClient(config: Config, val host: String) {
  val apiVersion = 16

  val port: Option[Int] = config.tryGetInt("port")
  val authToken: Option[String] = config.tryGetString("token")
  val transportSecurity: Option[TransportSecurity.Value] = config.tryGetString("transportSecurity").map(TransportSecurity.withName)

  private val requestTimeout: Duration = 5.seconds
  protected val baseWaitInterval: Duration = 100.milliseconds
  protected val terminationGlobalTimeout: Duration = 1.minute

  private val jsonRequestBuilder = RequestBuilder()
    .setHeader(HttpHeaders.ContentType, Message.ContentTypeJson)
    .setHeader(HttpHeaders.Accept, Message.ContentTypeJson)

  private val clientBuilder = new SingleNodeHttpClientBuilder(host, port, transportSecurity)
  protected val client: Request => Future[ConsumedResponse] = clientBuilder.build(requestTimeout)
  protected val clientForIdempotentRequests: Request => Future[ConsumedResponse] = clientBuilder.build(requestTimeout, areRequestsIdempotent = true)

  private def post(apiSubPath: String, body: Option[JsValue] = None, isIdempotent: Boolean = false): Future[ConsumedResponse] = {
    val cl = if (isIdempotent) clientForIdempotentRequests else client
    val req = clientBuilder
      .createRequest(apiSubPath, jsonRequestBuilder)
      .buildPost(body.map(_.compactPrint).map(Buf.Utf8(_)).getOrElse(Buf.Empty))
    cl(req)
  }

  private def apiPath(apiSubPath: String, queryParameters: Map[String, String] = Map()): String = {
    val path = s"/api/$apiVersion/$apiSubPath"
    val q = authToken.map(t => Map("authtoken" -> t)).getOrElse(Map()) ++ queryParameters
    if (q.nonEmpty) s"$path?${q.map { case (k, v) => s"$k=$v" }.mkString("&")}" else path
  }

  def startJob(jobName: String, parameters: Map[String, String] = Map()): Future[ConsumedResponse] = {
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
    post(apiPath(s"execution/$jobId/abort"), isIdempotent = true).flatMap(resp =>
      resp.status match {
        case NotFound =>
          Future.value(RundeckJobState.notFound)
        case Ok =>
          val body = resp.contentJson.asJsObject
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
    post(apiPath(s"execution/$jobId/output/state", Map("stateOnly" -> "true")), isIdempotent = true).map(resp =>
      resp.status match {
        case NotFound => RundeckJobState.notFound
        case Ok if isJobCompleted(resp.contentJson.asJsObject) => RundeckJobState.terminated
        case Ok => RundeckJobState.running
        case error => throw new RuntimeException(s"Rundeck error (${error.code}): ${error.reason}")
        // todo: return a status and a reason from the stopper
      }
    )

  private def waitForJobFinalState(jobId: String): Future[RundeckJobState.ExecState] = {
    val deadlineInNs = System.nanoTime() + terminationGlobalTimeout.inNanoseconds

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
