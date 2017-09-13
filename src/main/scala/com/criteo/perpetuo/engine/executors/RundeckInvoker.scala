package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.model.Version
import com.twitter.finagle.http.{Message, Method, Request}
import spray.json.DefaultJsonProtocol._
import spray.json._

class RundeckInvoker(name: String, host: String, port: Int, val apiVersion: Int, val authToken: String) extends HttpInvoker(host, port, name) {

  private def authenticated(path: String): String =
    s"$path?authtoken=$authToken"

  private def runPath(jobName: String): String =
    authenticated(s"/api/$apiVersion/job/$jobName/executions")

  def buildRequest(execTraceId: Long, executionKind: String, productName: String, version: Version, target: String, frozenParameters: String, initiator: String): Request = {
    val parameters = frozenParameters.parseJson.asJsObject.fields

    def squote(s: String) = s"'$s'"

    var quotedVersion = version.toString
    if (quotedVersion.startsWith("["))
      quotedVersion = squote(quotedVersion)

    // Rundeck before API version 18 do not support invocation with structured arguments
    val args = Map(
      "callback-url" -> squote(callbackUrl(execTraceId)),
      "product-name" -> squote(productName),
      "target" -> squote(target),
      "product-version" -> quotedVersion
    ) ++ parameters.collect { case (parameterName, value) if parameterName != "jobName" =>
        parameterName.replaceAll("([A-Z])", "-$1").toLowerCase -> value
    }

    val argString = args.toStream
      .map { case (parameterName, value) =>
        s"-$parameterName $value"
      }
      .mkString(" ")

    val body = Map("argString" -> argString).toJson

    val jobName = parameters("jobName").asInstanceOf[JsString].value
    val req = Request(Method.Post, runPath(jobName))
    req.host = host
    req.contentType = Message.ContentTypeJson
    req.accept = Message.ContentTypeJson // default response format is XML
    req.contentString = body.compactPrint
    req
  }

  def extractLogHref(executorAnswer: String): String =
    executorAnswer.parseJson.asJsObject.fields("permalink").asInstanceOf[JsString].value

  val ERROR_IN_HTML = """.+<p>(.+)</p>.+""".r

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
