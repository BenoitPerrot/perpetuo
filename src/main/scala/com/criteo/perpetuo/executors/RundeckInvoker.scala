package com.criteo.perpetuo.executors

import java.net.InetSocketAddress

import com.criteo.perpetuo.app.AppConfig
import com.criteo.perpetuo.model.Operation.Operation
import com.criteo.perpetuo.model.Version
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{Fields, _}
import com.twitter.finagle.service.{Backoff, RetryPolicy}
import com.twitter.inject.Logging
import com.twitter.util.{Await, Future => TwitterFuture, TimeoutException => TwitterTimeout}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ScalaFuture, TimeoutException => ScalaTimeout}
import scala.util.Try
import scala.util.matching.Regex


class RundeckInvoker(val host: String,
                     val port: Int,
                     val name: String,
                     val marathonEnv: String,
                     val forceSsl: Boolean = false) extends ExecutorInvoker with Logging {
  // how Rundeck is currently configured
  protected def jobName(operation: Operation): String = "deploy-to-marathon"
  protected val apiVersion = 16

  // authentication
  protected val authToken: String = AppConfig.under("tokens").get(name)

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


  override def toString: String = name

  override def trigger(operation: Operation, executionId: Long, productName: String, version: Version, rawTarget: String, initiator: String): Some[ScalaFuture[String]] = {
    // todo: while we only deal with marathon target, we directly give the select dimension, and formatted differently
    val degenerateTarget = Try(
      rawTarget.parseJson
        .asInstanceOf[JsArray].elements.head
        .asJsObject.fields("select")
        .asInstanceOf[JsArray].elements
        .map(_.asInstanceOf[JsString].value)
        .mkString(","))
      .getOrElse(rawTarget) // todo: remove it

    assert(!productName.contains("'"))
    assert(!version.value.contains("'"))
    val escapedTarget = degenerateTarget.toJson.compactPrint
    val body = Map(
      // before version 18 of Rundeck, we can't pass options in a structured way
      "argString" -> (
          s"-environment $marathonEnv " + // todo: remove it?
          s"-callback-url '${callbackUrl(executionId)}' " +
          s"-product-name '$productName' " +
          s"-product-version '$version' " +
          s"-target $escapedTarget")
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

    // trigger the job and return a future to the execution's log href
    Some(ScalaFuture {
      // convert a twitter Future to a scala one, as well as the possibly induced timeout exception
      val response = try {
        Await.result(client(req), totalTimeout + 1.second)
      } catch {
        case e: TwitterTimeout => throw new ScalaTimeout(e.getMessage)
      }

      val content = response.contentString
      response.status match {
        case Status.Successful(_) =>
          // return the permalink to this job run as the execution's log href
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
