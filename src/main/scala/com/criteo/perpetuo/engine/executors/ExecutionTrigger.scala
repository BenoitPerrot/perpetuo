package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.ExecutionState.ExecutionState
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging

import scala.concurrent.Future


/**
  * An instance is supposed to be dedicated to an executor or a type of executor
  * and is able to trigger an execution on it.
  */
trait ExecutionTrigger {
  /**
    * Trigger a new execution.
    *
    * @return a possible log href to uniquely identify the execution, if available.
    */
  def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]]

  /**
    * The executorName should be stable because it's persisted in the DB and used
    * later to instantiate the right TriggeredExecution from a log href in order
    * to interact with an execution.
    */
  val executorName: String
}


/**
  * Reflect an execution that has been triggered on an executor.
  * An instantiation must not try to actually reach the executor; each method does.
  */
trait TriggeredExecution {
  /**
    * To forcefully stop an execution if supported.
    *
    * @return a function if stopping is supported; the function takes no argument and returns the state
    *         of the execution (along with a reason, possibly empty) after the stop was attempted:
    *         - 'pending' if it is impossible to kill the execution because it is not started yet
    *         - 'running' if the execution could not be killed and is still seen as running
    *         - 'unreachable' if it is impossible to interact with the execution (permanently lost)
    *         - 'killed' if it was successfully stopped while running
    *         - None if it was already stopped (converted by the caller to 'unresolved' if appropriate)
    */
  val stopper: Option[() => Option[(ExecutionState, String)]]
}


// default no-op implementation of an execution trigger, whose execution is for now seen as running forever
// todo: add support for synchronous executors to ease the integration of trivial deployments? they would immediately return their final status
class DummyExecutionTrigger(name: String) extends ExecutionTrigger with Logging {
  override def toString: String = name

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$execTraceId on behalf of: $initiator")
    Future.successful(None)
  }

  override val executorName: String = "dummy"
}
