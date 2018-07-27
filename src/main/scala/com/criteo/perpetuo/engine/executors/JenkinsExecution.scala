package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.model.ExecutionState.ExecutionState


class JenkinsExecution(val logHref: String) extends TriggeredExecution {
  val host = {
    val matcher = JenkinsExecution.logHrefPattern.matcher(logHref)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Jenkins executor from $logHref")

    matcher.group(1)
  }

  override val stopper: Option[() => Option[ExecutionState]] = Some(() => None)
}

private object JenkinsExecution {
  val logHrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/.+/([0-9]+)")
}


