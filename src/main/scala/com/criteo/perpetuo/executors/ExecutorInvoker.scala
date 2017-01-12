package com.criteo.perpetuo.executors

import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.dispatchers.{Select, Tactics}
import com.twitter.inject.Logging

import scala.concurrent.Future


abstract class ExecutorInvoker {
  def trigger(operation: Operation, executionId: Long, productName: String, version: String, tactics: Tactics, select: Select, initiator: String): Option[Future[String]]
}


class DummyInvoker(name: String) extends ExecutorInvoker with Logging {
  override def toString: String = name

  override def trigger(operation: Operation, executionId: Long, productName: String, version: String, tactics: Tactics, select: Select, initiator: String): Option[Future[String]] = {
    logger.info(s"Hi, I'm $name!")
    logger.info(s"I will pretend to $operation (#$executionId) $productName-$version")
    logger.info(s" - to: ${select.mkString(", ")}")
    logger.info(s" - using tactics:\n    ${tactics.map(_.prettyPrint).mkString("\n    ")}")
    logger.info(s" - on behalf of: $initiator")
    None
  }
}
