package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.app.RestApi
import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.engine.dispatchers.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging

import scala.collection.mutable
import scala.concurrent.Future


abstract class ExecutorInvoker {
  ExecutorInvoker.registeredInvokers += this

  protected def callbackUrl(execTraceId: Long): String = AppConfigProvider.config.getString("selfUrl") + RestApi.executionCallbackPath(execTraceId.toString)

  def trigger(execTraceId: Long, executionKind: String, productName: String, version: Version, target: TargetExpr, frozenParameters: String, initiator: String): Future[Option[String]]

  def getExecutionDetailsUrlIfApplicable(logHref: String): Option[String] = None
}


object ExecutorInvoker {
  protected val registeredInvokers: mutable.Set[ExecutorInvoker] = mutable.Set()

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

  override def trigger(execTraceId: Long, executionKind: String, productName: String, version: Version, target: TargetExpr, frozenParameters: String, initiator: String): Future[Option[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$execTraceId on behalf of: $initiator")
    Future.successful(None)
  }
}


class UnprocessableIntent extends IllegalArgumentException
