package com.criteo.perpetuo.engine.invokers

import com.criteo.perpetuo.model.Version
import com.twitter.finagle.http.{Message, Method, Request}
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.{Map => MapBase}

class RundeckInvoker(name: String, host: String, port: Int, authToken: String, jobName: String, specificParameters: MapBase[String, String] = Map()) extends HttpInvoker(host, port, name) {
  val API_VERSION = 16

  private def authenticated(path: String): String =
    s"$path?authtoken=$authToken"

  private def runPath(jobName: String): String =
    authenticated(s"/api/$API_VERSION/job/$jobName/executions")

  def buildRequest(execTraceId: Long, executionKind: String, productName: String, version: Version, target: String, initiator: String): Request = {
    def squote(s: String) = s"'$s'"

    var quotedVersion = version.toString
    if (quotedVersion.startsWith("["))
      quotedVersion = squote(quotedVersion)

    // Rundeck before API version 18 does not support invocation with structured arguments
    val args = Map(
      "callback-url" -> squote(callbackUrl(execTraceId)),
      "product-name" -> squote(productName),
      "target" -> squote(target),
      "product-version" -> quotedVersion
    ) ++ specificParameters.map { case (parameterName, value) =>
      parameterName.replaceAll("([A-Z])", "-$1").toLowerCase -> value
    }

    val argString = args.toStream
      .map { case (parameterName, value) =>
        s"-$parameterName $value"
      }
      .mkString(" ")

    val body = Map("argString" -> argString).toJson

    val req = Request(Method.Post, runPath(jobName))
    req.host = host
    req.contentType = Message.ContentTypeJson
    req.accept = Message.ContentTypeJson // default response format is XML
    req.contentString = body.compactPrint
    req
  }

  def extractLogHref(executorAnswer: String): String =
    executorAnswer.parseJson.asJsObject.fields("permalink").asInstanceOf[JsString].value

  private val ERROR_IN_HTML = """.+<p>(.+)</p>.+""".r

  def extractMessage(status: Int, content: String): String = {
    try {
      content.parseJson.asJsObject.fields("message").asInstanceOf[JsString].value
    } catch {
      case _: Exception =>
        content match {
          case ERROR_IN_HTML(message) => message
          case _ => ""
        }
    }
  }
}
