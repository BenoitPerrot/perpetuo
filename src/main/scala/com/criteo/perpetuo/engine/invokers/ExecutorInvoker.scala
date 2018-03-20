package com.criteo.perpetuo.engine.invokers

import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging

import scala.concurrent.Future


abstract class ExecutorInvoker {
  def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]]
}


class DummyInvoker(name: String) extends ExecutorInvoker with Logging {
  override def toString: String = name

  override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$execTraceId on behalf of: $initiator")
    Future.successful(None)
  }
}
