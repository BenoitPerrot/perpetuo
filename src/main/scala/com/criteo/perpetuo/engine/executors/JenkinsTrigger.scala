package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.app.RestApi
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.model.Version
import com.twitter.conversions.time._
import com.twitter.finagle.http.Status
import com.twitter.util.Await

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JenkinsTrigger(name: String,
                     val jobToken: Option[String],
                     val host: String,
                     jobName: String) extends ExecutionTrigger {

  override def toString: String = s"$name (job: $jobName)"

  val config = AppConfigProvider.executorConfig("jenkins")

  protected val client: JenkinsClient = new JenkinsClient(host)
  private val requestTimeout = 20.seconds

  /**
    * Trigger a new execution.
    *
    * @return None. Jenkins doesn't return any href immediately.
    */
  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetAtomSet, initiator: String): Future[Option[String]] = {

    val parameters = Map(
      "callbackUrl" -> RestApi.executionCallbackUrl(execTraceId),
      "productName" -> productName
    )

    Future {
      val response = Await.result(client.startJob(jobName, jobToken, parameters), requestTimeout + 1.second)

      response.status match {
        case Status.Successful(_) =>
          None
        case s =>
          throw new Exception(s"Bad response from $this: ${s.reason}")
      }
    }
  }

  override val executorType: String = "jenkins"
}
