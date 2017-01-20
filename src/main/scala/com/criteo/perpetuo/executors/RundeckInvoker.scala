package com.criteo.perpetuo.executors

import java.net.InetSocketAddress

import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Fields, _}
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.inject.Logging
import com.twitter.util.{Await, Future => TwitterFuture}
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ScalaFuture}
import scala.util.Try
import scala.util.matching.Regex


class RundeckInvoker(val host: String, val port: Int, val forceSsl: Boolean = false) extends ExecutorInvoker with Logging {
  private val config = ConfigFactory.load()

  // how Rundeck is currently configured
  protected val apiVersion = 16
  lazy protected val authToken: String = config.getString("rundeckAuthToken")
  protected def jobName(operation: Operation): String = operation.toString

  // Rundeck's API
  private def authenticated(path: String) = s"$path?authtoken=$authToken"
  private def runPath(operation: Operation) = authenticated(s"/api/$apiVersion/job/${jobName(operation)}/executions")
  private val errorInHtml: Regex = ".+<p>(.+)</p>.+".r

  // HTTP client
  private val ssl = forceSsl || port == 443
  private val totalTimeout = 20.seconds
  private val backoffDurations = Backoff.exponentialJittered(1.seconds, 5.seconds)
  private val backoffPolicy = RetryPolicy.backoff(backoffDurations)(RetryPolicy.TimeoutAndWriteExceptionsOnly)
  protected val client: (Request) => TwitterFuture[Response] = (if (ssl) ClientBuilder().tlsWithoutValidation else ClientBuilder())
    .codec(Http())
    .timeout(totalTimeout)
    .hostConnectionLimit(10)
    .hosts(new InetSocketAddress(host, port))
    .retryPolicy(backoffPolicy)
    .failFast(false)
    .build()


  override def toString: String = host

  override def trigger(operation: Operation, executionId: Long, productName: String, version: String, rawTarget: String, initiator: String): Some[ScalaFuture[String]] = {
    // before version 18 of Rundeck, we can't pass options in a structured way
    val Seq(escapedProductName, escapedVersion, escapedRawTarget) = Seq(productName, version, rawTarget).map((x: String) => x.toJson.compactPrint)
    val body = Map(
      "argString" -> s"-executionId $executionId -productName $escapedProductName -version $escapedVersion -rawTarget $escapedRawTarget"
      // todo? "asUser" -> initiator
    ).toJson.compactPrint

    // build the request
    val req = Request(Method.Post, runPath(operation))
    req.headerMap ++= Seq(
      Fields.Host -> host,
      Fields.ContentType -> Message.ContentTypeJson,
      Fields.Accept -> Message.ContentTypeJson // default response format is XML
    )
    req.write(body)

    // trigger the job and return a future to the execution's UUID
    Some(ScalaFuture {
      // convert a twitter Future to a scala one
      val response = Await.result(client(req), totalTimeout + 1.second)

      val content = response.contentString
      response.status match {
        case Status.Successful(_) =>
          // return the permalink to this job run as the execution's UUID
          logger.warn(content)
          content.parseJson.asJsObject.fields("permalink").asInstanceOf[JsString].value
        case s =>
          val embeddedDetail = Try(content.parseJson)
            .map(json => Try(json.asJsObject.fields("message")).map(jsonMsg => Some(jsonMsg.toString)).getOrElse(None))
            .getOrElse(content match {
              case errorInHtml(errMsg) => Some(errMsg)
              case _ => None
            })
          throw new Exception("Rundeck answered: " + embeddedDetail.map(detail => s"${s.reason}: $detail").getOrElse(s.reason))
      }
    })
  }
}
