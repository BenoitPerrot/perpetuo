package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.google.inject.Injector
import com.twitter.util.{Await, Future}


class RundeckExecution(client: RundeckClient, val executionNumber: Int, val href: String) extends TriggeredExecution {
  private def abortJob(jobId: String): Future[Option[ExecutionState]] =
    client.abortJob(jobId).map {
      case RundeckJobState.notFound => Some(ExecutionState.unreachable) // Rundeck doesn't know the execution, it is lost
      case RundeckJobState.running => Some(ExecutionState.running) // Unable to stop the job, it is still running
      case _ => None
    }

  override val stopper: Option[() => Option[ExecutionState]] = Some(() =>
    Await.result(abortJob(executionNumber.toString))
  )
}

class RundeckExecutionFactory(injector: Injector) extends TriggeredExecutionFactory {
  private val config = injector.getInstance(classOf[AppConfig]).executorConfig("rundeck")

  def apply(href: String): RundeckExecution = {
    val (host, executionNumber) = RundeckExecution.parseHref(href)
    val client = new RundeckClient(host, config.tryGetInt("port"), config.tryGetBoolean("ssl"), config.tryGetString("token"))
    new RundeckExecution(client, executionNumber, href)
  }
}

private object RundeckExecution {
  val hrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/.+/([0-9]+)")

  /** Return the host name and the execution number out of the href of a Rundeck execution */
  def parseHref(href: String): (String, Int) = {
    val matcher = hrefPattern.matcher(href)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Rundeck executor from $href")

    (matcher.group(1), matcher.group(3).toInt)
  }
}
