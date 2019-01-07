package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.TargetAtomSet
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging

import scala.concurrent.Future


// default no-op implementation of an execution trigger, whose execution is for now seen as running forever
// todo: add support for synchronous executors to ease the integration of trivial deployments? they would immediately return their final status
class NoOpTrigger extends ExecutionTrigger with Logging {
  override def toString: String = "no-op executor"

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetAtomSet, initiator: String): Future[Option[String]] = {
    logger.info(s"No-op executor triggered for #$execTraceId on behalf of $initiator")
    Future.successful(None)
  }

  override val executorType: String = "noop"
}
