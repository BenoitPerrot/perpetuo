package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.model.Version
import com.twitter.util.Await
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JenkinsTrigger(client: JenkinsClient,
                     val jobToken: Option[String],
                     jobName: String) extends ExecutionTrigger {
  def this(config: Config) = this(
    new JenkinsClient(config.getString("host"), config.tryGetInt("port"), config.tryGetBoolean("ssl"), config.tryGetString("username"), config.tryGetString("password")),
    config.tryGetString("jobToken"),
    config.getString("jobName")
  )

  override def toString: String = s"Jenkins (on ${client.host}) (job: $jobName)"

  /**
    * Trigger a new execution.
    *
    * @return None. Jenkins doesn't return any href immediately.
    */
  override def trigger(executionCallbackUrl: String, productName: String, version: Version, target: TargetAtomSet, initiator: String): Future[Option[String]] = {

    val parameters = Map(
      "callbackUrl" -> executionCallbackUrl,
      "productName" -> productName
    )

    Future { // fixme: use a conversion
      Await.result(client.startJob(jobName, jobToken, parameters))
      None
    }
  }

  override val executorType: String = "jenkins"
}
