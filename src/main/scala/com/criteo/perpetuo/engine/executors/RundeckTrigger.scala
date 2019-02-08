package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.model._
import com.criteo.perpetuo.util.FutureConversions._
import com.twitter.finagle.http.Status
import com.typesafe.config.Config
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.Future


class RundeckTrigger(client: RundeckClient,
                     jobName: String,
                     specificParameters: Iterable[(String, String)] = Map()) extends ExecutionTrigger {
  def this(config: Config) = this(
    new RundeckClient(config.getString("host"), config.tryGetInt("port"), config.tryGetBoolean("ssl"), config.tryGetString("token")),
    config.getString("jobName")
  )

  override def toString: String = s"Rundeck (on ${client.hostName}) (job: $jobName)"

  def extractHref(executorAnswer: String): String =
    executorAnswer.parseJson.asJsObject.fields("permalink").asInstanceOf[JsString].value

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

  override def trigger(executionCallbackUrl: String, productName: String, version: Version, target: TargetAtomSet, initiator: String): Future[Option[String]] = {
    def squote(s: String) = s"'$s'"

    var quotedVersion = version.toString
    if (quotedVersion.startsWith("["))
      quotedVersion = squote(quotedVersion)

    val triggerParameters = Map(
      "callback-url" -> squote(executionCallbackUrl),
      "product-name" -> squote(productName),
      "target" -> squote(target.superset.mkString(",")),
      "product-version" -> quotedVersion
    ) ++ specificParameters.map { case (parameterName, value) =>
      parameterName.replaceAll("([A-Z])", "-$1").toLowerCase -> squote(value)
    }

    // trigger the job and return a future to the execution's href
    client.startJob(jobName, triggerParameters).map { response =>
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
  def fromJavaTypes(client: RundeckClient, jobName: String, specificParameters: java.util.Map[String, String]): RundeckTrigger =
    new RundeckTrigger(client, jobName, specificParameters)
}
