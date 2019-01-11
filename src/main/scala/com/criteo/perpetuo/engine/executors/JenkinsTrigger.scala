package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.app.RestApi
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.model.Version
import com.twitter.conversions.time._
import com.twitter.util.Await
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JenkinsTrigger(val jobToken: Option[String],
                     val host: String,
                     jobName: String) extends ExecutionTrigger {
  def this(config: Config) = this(
    config.tryGetString("jobToken"),
    config.getString("host"),
    config.getString("jobName")
  )

  override def toString: String = s"Jenkins (on $host) (job: $jobName)"

  protected val client: JenkinsClient = new JenkinsClient(AppConfig.executorConfig("jenkins"), host)
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

    Future { // fixme: use raiseWithin and a conversion?
      Await.result(client.startJob(jobName, jobToken, parameters), requestTimeout + 1.second)
      None
    }
  }

  override val executorType: String = "jenkins"
}
