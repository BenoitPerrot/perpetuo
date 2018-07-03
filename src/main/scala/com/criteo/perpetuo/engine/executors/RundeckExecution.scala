package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.twitter.util.{Await, Future}


class RundeckExecution(val logHref: String) extends TriggeredExecution {
  val (host, executionNumber) = {
    val matcher = RundeckExecution.logHrefPattern.matcher(logHref)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Rundeck executor from $logHref")

    (matcher.group(1), matcher.group(3).toInt)
  }

  protected val client: RundeckClient = new RundeckClient(host)

  private def abortJob(jobId: String): Future[Option[ExecutionState]] =
    client.abortJob(jobId).map {
      case RundeckJobState.notFound => Some(ExecutionState.unreachable) // Rundeck doesn't know the execution, it is lost
      case RundeckJobState.running => Some(ExecutionState.running) // Unable to stop the job, it is still running
      case _ => None
    }

  override val stopper: Option[() => Option[ExecutionState]] = Some(() =>
    Await.result(abortJob(executionNumber.toString), client.maxAbortDuration)
  )
}


private object RundeckExecution {
  val logHrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/.+/([0-9]+)")
}
