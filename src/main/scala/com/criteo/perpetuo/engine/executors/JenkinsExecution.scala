package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.google.inject.Injector
import com.twitter.util.{Await, Future}
import com.typesafe.config.Config


class JenkinsExecution(config: Config, val href: String) extends TriggeredExecution {
  val (host, jobName, buildId) = {
    val matcher = JenkinsExecution.hrefPattern.matcher(href)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Jenkins executor from $href")

    (matcher.group(1), matcher.group(3), matcher.group(4))
  }

  protected val client: JenkinsClient = new JenkinsClient(config, host)

  private def abortJob(jobName: String, jobId: String): Future[Option[ExecutionState]] =
    client.abortJob(jobName, jobId).map {
      case JenkinsJobState.notFound => Some(ExecutionState.unreachable)
      case _ => None
    }

  override val stopper: Option[() => Option[ExecutionState]] = Some(() =>
    Await.result(abortJob(jobName, buildId), client.maxAbortDuration)
  )
}

class JenkinsExecutionFactory(injector: Injector) extends TriggeredExecutionFactory {
  private val config = injector.getInstance(classOf[AppConfig]).executorConfig("jenkins")

  def apply(href: String): JenkinsExecution =
    new JenkinsExecution(config, href)
}

private object JenkinsExecution {
  val hrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/job/([^/]+)/([0-9]+)/?")
}


