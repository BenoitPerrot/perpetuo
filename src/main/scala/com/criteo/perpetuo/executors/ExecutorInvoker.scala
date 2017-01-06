package com.criteo.perpetuo.executors

import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.dispatchers.{Select, Tactics}
import com.twitter.inject.Logging
import com.twitter.util.Future


abstract class ExecutorInvoker {
  def trigger(operation: Operation, tactics: Tactics, select: Select): Option[Future[String]]
}


class DummyInvoker(name: String) extends ExecutorInvoker with Logging {
  override def toString: String = name

  override def trigger(operation: Operation, tactics: Tactics, select: Select): Option[Future[String]] = {
    logger.info(s"Hi, I'm $name:")
    logger.info(s"- I will pretend to $operation to: ${select.mkString(", ")}")
    logger.info(s"- Using tactics: ${tactics.prettyPrint}")
    None
  }
}
