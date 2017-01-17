package com.criteo.perpetuo.executors

import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.twitter.inject.Logging

import scala.concurrent.Future


abstract class ExecutorInvoker {
  def trigger(operation: Operation, executionId: Long, productName: String, version: String, rawTarget: String, initiator: String): Option[Future[String]]
}


class DummyInvoker(name: String) extends ExecutorInvoker with Logging {
  override def toString: String = name

  override def trigger(operation: Operation, executionId: Long, productName: String, version: String, rawTarget: String, initiator: String): Option[Future[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$executionId on behalf of: $initiator")
    None
  }
}
