package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging
import com.typesafe.config.Config

import scala.concurrent.Future


// default no-op implementation of an execution trigger, whose execution is for now seen as running forever
// todo: add support for synchronous executors to ease the integration of trivial deployments? they would immediately return their final status
class DummyExecutionTrigger(name: String) extends ExecutionTrigger with Logging {
  def this(config: Config) {
    this(config.getString("name"))
  }

  override def toString: String = name

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$execTraceId on behalf of: $initiator")
    Future.successful(None)
  }

  override val executorName: String = "dummy"
}
