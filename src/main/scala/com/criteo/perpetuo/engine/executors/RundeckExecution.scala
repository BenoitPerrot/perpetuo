package com.criteo.perpetuo.engine.executors

import java.util.regex.Pattern

import com.criteo.perpetuo.model.ExecutionState.ExecutionState


class RundeckExecution(val logHref: String) extends TriggeredExecution {
  val (host, executionNumber) = {
    val matcher = RundeckExecution.logHrefPattern.matcher(logHref)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Cannot find a proper Rundeck executor from $logHref")

    (matcher.group(1), matcher.group(3).toInt)
  }

 override val stopper: Option[() => Option[(ExecutionState, String)]] = None
}


private object RundeckExecution {
  val logHrefPattern: Pattern = Pattern.compile("https?://([^/:]+)(:[0-9]+)?/.+/([0-9]+)")
}
