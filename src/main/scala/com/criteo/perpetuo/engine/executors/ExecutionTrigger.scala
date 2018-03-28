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
trait Trigger {
  def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]]
}

trait Stop {
    /**
      * Forcefully stop an execution
      *
      * @return the final state of the execution:
      *         - 'pending' if it is impossible to kill the execution because it is not started yet
      *         - 'running' if the execution could not be killed and is still seen as running
      *         - 'unreachable' if it is impossible to interact with the execution (permanently lost)
      *         - 'killed' if it was successfully stopped while running
      *         - None if it was already stopped (converted by the caller to 'unresolved' if appropriate)
      */
  def stop(execTraceId: Long): Option[ExecutionState]
}


// to be removed
sealed trait BaseExecutionTrigger extends Trigger {
  val stopper: Option[Long => Option[ExecutionState]]
}


trait ExecutionTrigger extends BaseExecutionTrigger {
  override final val stopper = None
}


// default no-op implementation of an execution trigger, whose execution is for now seen as running forever
// todo: add support for synchronous executors to ease the integration of trivial deployments? they would immediately return their final status
class DummyExecutionTrigger(name: String) extends ExecutionTrigger with Logging {
  override def toString: String = name

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$execTraceId on behalf of: $initiator")
    Future.successful(None)
  }
}
