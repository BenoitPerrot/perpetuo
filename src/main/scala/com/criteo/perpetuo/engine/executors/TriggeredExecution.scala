package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.model.ExecutionState.ExecutionState


/**
  * Reflect an execution that has been triggered on an executor.
  * Any implementation of it must have a constructor taking a log href as single parameter.
  * An instantiation must not try to actually reach the executor; each method does.
  * An instantiation must not fail, but must instead consider the execution as unreachable
  * (even for instance if the arguments given to the constructor make no sense).
  */
trait TriggeredExecution {
  val logHref: String // todo: find another name for logHref (rename all occurrences) now that it's used to more generally interact with executions

  /**
    * To forcefully stop an execution if supported.
    *
    * @return a function if stopping is supported; the function takes no argument and returns the state
    *         of the execution (along with a reason, possibly empty) after the stop was attempted:
    *         - 'pending' if it is impossible to stop the execution because it is not started yet
    *         - 'running' if the execution could not be stopped and is still seen as running
    *         - 'unreachable' if it is impossible to interact with the execution (permanently lost)
    *         - 'stopped' if it was successfully stopped while running
    *         - None if it was already stopped (converted by the caller to 'unresolved' if appropriate)
    */
  val stopper: Option[() => Option[(ExecutionState, String)]]
}


class UncontrollableTriggeredExecution(val logHref: String) extends TriggeredExecution {
  override val stopper: Option[() => Option[(ExecutionState, String)]] = None
}
