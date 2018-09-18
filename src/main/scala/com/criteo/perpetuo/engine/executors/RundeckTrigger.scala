package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.app.RestApi
import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.conversions.time._
import com.twitter.finagle.http.Status
import com.twitter.util.Await
import com.typesafe.config.Config
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class RundeckTrigger(name: String,
                     val host: String,
                     jobName: String,
                     specificParameters: Iterable[(String, String)] = Map()) extends ExecutionTrigger {
  def this(config: Config) {
    this(
      config.getString("name"),
      config.getString("host"),
      config.getString("jobName")
    )
  }

  override def toString: String = s"$name (job: $jobName)"

  protected val client: RundeckClient = new RundeckClient(host)

  def extractHref(executorAnswer: String): String =
    executorAnswer.parseJson.asJsObject.fields("permalink").asInstanceOf[JsString].value

  private val requestTimeout = 20.seconds

  private val ERROR_IN_HTML = """.+<p>(.+)</p>.+""".r

  def extractMessage(content: String): String = {
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

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    // todo: while we only support deployment tactics, we directly give the select dimension, and formatted differently

    def squote(s: String) = s"'$s'"

    var quotedVersion = version.toString
    if (quotedVersion.startsWith("["))
      quotedVersion = squote(quotedVersion)

    val triggerParameters = Map(
      "callback-url" -> squote(RestApi.executionCallbackUrl(execTraceId)),
      "product-name" -> squote(productName),
      "target" -> squote(target.mkString(",")),
      "product-version" -> quotedVersion
    ) ++ specificParameters.map { case (parameterName, value) =>
      parameterName.replaceAll("([A-Z])", "-$1").toLowerCase -> value
    }

    // trigger the job and return a future to the execution's href
    Future {
      // convert a twitter Future to a scala one
      val response = Await.result(client.startJob(jobName, triggerParameters), requestTimeout + 1.second)

      val content = response.contentString
      response.status match {
        case Status.Successful(_) =>
          val href = extractHref(content)
          if (href.nonEmpty) Some(href) else None
        case s =>
          val embeddedDetail = extractMessage(content)
          val detail = if (embeddedDetail.nonEmpty) s"${s.reason}: $embeddedDetail" else s.reason
          throw new Exception(s"Bad response from $this: " + detail)
      }
    }
  }

  override val executorType: String = "rundeck"
}


object RundeckTrigger {
  def fromJavaTypes(name: String, host: String, jobName: String, specificParameters: java.util.Map[String, String]): RundeckTrigger =
    new RundeckTrigger(name, host, jobName, specificParameters.asScala)
}
