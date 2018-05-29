package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.app.RestApi
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.model.Version
import com.twitter.finagle.http.{Message, Method, Request}
import com.typesafe.config.Config
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.JavaConverters._


class RundeckTrigger(name: String,
                     val host: String,
                     val port: Int,
                     val authToken: Option[String],
                     jobName: String,
                     specificParameters: Iterable[(String, String)] = Map()) extends ExecutionHttpTrigger {
  def this(config: Config) {
    this(
      config.getString("name"),
      config.getString("host"),
      config.getIntOrElse("port", 80),
      config.tryGetString("token"),
      config.getString("jobName")
    )
  }

  val API_VERSION = 16

  override def toString: String = s"$name (job: $jobName)"

  private def apiPath(apiSubPath: String): String = {
    val path = s"/api/$API_VERSION/$apiSubPath"
    authToken.map(t => s"$path?authtoken=$t").getOrElse(path)
  }

  private def runPath(jobName: String): String =
    apiPath(s"job/$jobName/executions")

  protected def buildRequest(execTraceId: Long, productName: String, version: Version, target: String, initiator: String): Request = {
    def squote(s: String) = s"'$s'"

    var quotedVersion = version.toString
    if (quotedVersion.startsWith("["))
      quotedVersion = squote(quotedVersion)

    // Rundeck before API version 18 does not support invocation with structured arguments
    val args = Map(
      "callback-url" -> squote(RestApi.executionCallbackUrl(execTraceId)),
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

  override val executorName: String = "rundeck"
}


object RundeckTrigger {
  def fromJavaTypes(name: String, host: String, port: Int, authToken: String, jobName: String, specificParameters: java.util.Map[String, String]) =
    new RundeckTrigger(name, host, port, Option(authToken), jobName, specificParameters.asScala)
}
