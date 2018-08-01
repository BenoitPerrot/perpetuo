package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.twitter.util.{Await, Future}


class JenkinsExecution(val logHref: String) extends TriggeredExecution {
  val (host, jobName, buildId) = {
    val matcher = JenkinsExecution.logHrefPattern.matcher(logHref)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Jenkins executor from $logHref")

    (matcher.group(1), matcher.group(3), matcher.group(4))
  }

  protected val client: JenkinsClient = new JenkinsClient(host)

  private def abortJob(jobName: String, jobId: String): Future[Option[ExecutionState]] =
    client.abortJob(jobName, jobId).map {
      case JenkinsJobState.notFound => Some(ExecutionState.unreachable)
      case _ => None
    }

  override val stopper: Option[() => Option[ExecutionState]] = Some(() =>
    Await.result(abortJob(jobName, buildId), client.maxAbortDuration)
  )
}

private object JenkinsExecution {
  val logHrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/job/([^/]+)/([0-9]+)/?")
}


