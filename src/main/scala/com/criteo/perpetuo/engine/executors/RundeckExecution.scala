package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.google.inject.Injector
import com.twitter.util.{Await, Future}
import com.typesafe.config.Config


class RundeckExecution(config: Config, val href: String) extends TriggeredExecution {
  val (host, executionNumber) = {
    val matcher = RundeckExecution.hrefPattern.matcher(href)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Rundeck executor from $href")

    (matcher.group(1), matcher.group(3).toInt)
  }

  protected val client: RundeckClient = new RundeckClient(host, config.tryGetInt("port"), config.tryGetBoolean("ssl"), config.tryGetString("token"))

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

  def apply(href: String): RundeckExecution =
    new RundeckExecution(config, href)
}

private object RundeckExecution {
  val hrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/.+/([0-9]+)")
}
