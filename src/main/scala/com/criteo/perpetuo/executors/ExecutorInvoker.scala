package com.criteo.perpetuo.executors

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.dispatchers.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging

import scala.collection.mutable
import scala.concurrent.Future


abstract class ExecutorInvoker {
  ExecutorInvoker.registeredInvokers += this

  protected def callbackUrl(executionId: Long): String = s"${ExecutorInvoker.selfUrl}/api/execution-traces/$executionId"

  def trigger(operationName: String, executionId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Option[Future[String]]

  def getExecutionDetailsUrlIfApplicable(logHref: String): Option[String] = None
}


object ExecutorInvoker {
  protected val registeredInvokers: mutable.Set[ExecutorInvoker] = mutable.Set()

  protected val selfUrl: String = AppConfig.get("selfUrl")

  def getExecutionDetailsUrl(logHref: String): String = {
    if (logHref.contains("://"))
      logHref
    else {
      val urls = ExecutorInvoker.registeredInvokers.flatMap(_.getExecutionDetailsUrlIfApplicable(logHref))
      if (urls.size != 1)
        throw new Exception(
          if (urls.isEmpty) s"Could not interpret log href `$logHref`"
          else s"Multiple interpretations for `$logHref`: ${urls.mkString(", ")}"
        )
      urls.head
    }
  }
}


class DummyInvoker(name: String) extends ExecutorInvoker with Logging {
  override def toString: String = name

  override def trigger(operationName: String, executionId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Option[Future[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$executionId on behalf of: $initiator")
    None
  }
}
