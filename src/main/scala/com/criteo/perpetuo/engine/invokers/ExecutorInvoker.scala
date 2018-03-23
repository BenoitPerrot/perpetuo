package com.criteo.perpetuo.engine.invokers

import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging

import scala.concurrent.Future


trait Trigger {
  def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]]
}


sealed trait ExecutorInvoker extends Trigger

trait UnstoppableInvoker extends ExecutorInvoker


// default no-op implementation of an invoker, whose execution is for now seen as running forever
// todo: add support for synchronous invokers to ease the integration of trivial deployments? they would immediately return their final status
class DummyUnstoppableInvoker(name: String) extends UnstoppableInvoker with Logging {
  override def toString: String = name

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$execTraceId on behalf of: $initiator")
    Future.successful(None)
  }
}
