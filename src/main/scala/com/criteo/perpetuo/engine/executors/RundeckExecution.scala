package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.twitter.util.{Await, Future}


class RundeckExecution(val href: String) extends TriggeredExecution {
  val (host, executionNumber) = {
    val matcher = RundeckExecution.hrefPattern.matcher(href)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Rundeck executor from $href")

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

class RundeckExecutionFactory extends TriggeredExecutionFactory {
  def apply(href: String): RundeckExecution =
    new RundeckExecution(href)
}

private object RundeckExecution {
  val hrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/.+/([0-9]+)")
}
