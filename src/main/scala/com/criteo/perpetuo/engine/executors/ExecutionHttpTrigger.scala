package com.criteo.perpetuo.engine.executors

import java.net.InetSocketAddress

import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.{Target, Version}
import com.twitter.conversions.time._
import com.twitter.finagle.Http.Client
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.util.{Await, Duration, Future => TwitterFuture, Try => TwitterTry}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


trait ExecutionHttpTrigger extends ExecutionTrigger {
  val host: String
  val port: Int

  // to implement in concrete classes
  /** `buildRequest` returns the HTTP request object ready to invoke the appropriate executor in charge of running the execution. */
  protected def buildRequest(execTraceId: Long, productName: String, version: Version, target: String, initiator: String): Request
  /** `logHref` gives a unique identifier allowing to find possible external execution logs. */
  protected def extractLogHref(executorAnswer: String): String // answer "" if no log href can be known (e.g. delayed execution)
  /** `extractMessage` extracts an error message from any error output returned by the contacted API. */
  protected def extractMessage(status: Int, content: String): String // answer "" if no message can be extracted

  // HTTP client
  protected val ssl: Boolean = port == 443
  protected val requestTimeout: Duration = 20.seconds
  protected val maxConnectionsPerHost: Int = 10
  protected val backoffDurations: Stream[Duration] = Backoff.exponentialJittered(1.seconds, 5.seconds)
  protected val backoffPolicy: RetryPolicy[TwitterTry[Nothing]] = RetryPolicy.backoff(backoffDurations)(RetryPolicy.TimeoutAndWriteExceptionsOnly)

  // todo: replace this by a protected [lazy] val and mock it in the utests
  var client: (Request) => TwitterFuture[Response] = (if (ssl) ClientBuilder().tlsWithoutValidation else ClientBuilder())
    .stack(Client())
    .timeout(requestTimeout)
    .hostConnectionLimit(maxConnectionsPerHost)
    .hosts(new InetSocketAddress(host, port))
    .retryPolicy(backoffPolicy)
    .failFast(false)
    .build()

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    // todo: while we only support deployment tactics, we directly give the select dimension, and formatted differently
    val req = buildRequest(execTraceId, productName, version, Target.getSimpleSelect(target).mkString(","), initiator)

    // trigger the job and return a future to the execution's log href
    Future {
      // convert a twitter Future to a scala one
      val response = Await.result(client(req), requestTimeout + 1.second)

      val content = response.contentString
      response.status match {
        case Status.Successful(_) =>
          val logHref = extractLogHref(content)
          if (logHref.nonEmpty) Some(logHref) else None
        case s =>
          val embeddedDetail = extractMessage(response.statusCode, content)
          val detail = if (embeddedDetail.nonEmpty) s"${s.reason}: $embeddedDetail" else s.reason
          throw new Exception(s"Bad response from $this: " + detail)
      }
    }
  }
}