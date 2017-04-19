package com.criteo.perpetuo.executors

import java.net.InetSocketAddress

import com.criteo.perpetuo.dispatchers.TargetExpr
import com.criteo.perpetuo.model.{Target, Version}
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http._
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.util.{Await, Duration, Future => TwitterFuture, TimeoutException => TwitterTimeout, Try => TwitterTry}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ScalaFuture, TimeoutException => ScalaTimeout}


abstract class HttpInvoker(val host: String,
                           val port: Int,
                           val name: String) extends ExecutorInvoker {

  // to implement in concrete classes
  protected def buildRequest(operationName: String, executionId: Long, productName: String, version: String, target: String, initiator: String): Request
  protected def getLogHref(executorAnswer: String): String
  protected def tryExtractMessage(status: Int, content: String): Option[String]

  // HTTP client
  protected val ssl: Boolean = port == 443
  protected val requestTimeout: Duration = 20.seconds
  protected val maxConnectionsPerHost: Int = 10
  protected val backoffDurations: Stream[Duration] = Backoff.exponentialJittered(1.seconds, 5.seconds)
  protected val backoffPolicy: RetryPolicy[TwitterTry[Nothing]] = RetryPolicy.backoff(backoffDurations)(RetryPolicy.TimeoutAndWriteExceptionsOnly)

  // todo: replace this by a protected [lazy] val and mock it in the utests
  var client: (Request) => TwitterFuture[Response] = (if (ssl) ClientBuilder().tlsWithoutValidation else ClientBuilder())
    .codec(Http())
    .timeout(requestTimeout)
    .hostConnectionLimit(maxConnectionsPerHost)
    .hosts(new InetSocketAddress(host, port))
    .retryPolicy(backoffPolicy)
    .failFast(false)
    .build()

  override def toString: String = name

  override def trigger(operationName: String, executionId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Some[ScalaFuture[String]] = {
    // todo: while we only deal with marathon target, we directly give the select dimension, and formatted differently
    val req = buildRequest(operationName, executionId, productName, version.toString, Target.getSimpleSelect(target).mkString(","), initiator)

    // trigger the job and return a future to the execution's log href
    Some(ScalaFuture {
      // convert a twitter Future to a scala one, as well as the possibly induced timeout exception
      val response = try {
        Await.result(client(req), requestTimeout + 1.second)
      } catch {
        case e: TwitterTimeout => throw new ScalaTimeout(e.getMessage)
      }

      val content = response.contentString
      response.status match {
        case Status.Successful(_) =>
          getLogHref(content)
        case s =>
          val embeddedDetail = tryExtractMessage(response.statusCode, content)
          throw new Exception("Rundeck answered: " + embeddedDetail.map(detail => s"${s.reason}: $detail").getOrElse(s.reason))
      }
    })
  }
}
