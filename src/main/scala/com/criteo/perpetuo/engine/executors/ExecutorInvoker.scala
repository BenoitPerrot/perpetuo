package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.app.RestApi
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.engine.dispatchers.TargetExpr
import com.twitter.inject.Logging
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable
import scala.concurrent.Future


abstract class ExecutorInvoker {
  ExecutorInvoker.registeredInvokers += this

  protected def callbackUrl(executionId: Long): String = AppConfig.get[String]("selfUrl") + RestApi.executionCallbackPath(executionId.toString)

  /**
    * `freezeParameters` must return the execution parameters serialized as they will be
    * provided to `trigger` in order to play or replay an execution in a deterministic way,
    * except that it must be replayable with a subset of the original target (so the targets
    * should not be included in the frozen parameters).
    * If the input doesn't make sense (the parameters are incompatible with each other),
    * it must return a `UnprocessableIntent` error, whose message will be displayed to the end user.
    */
  def freezeParameters(executionKind: String, productName: String, jsonVersion: String): String = JsObject(
    "executionKind" -> executionKind.toJson,
    "productName" -> productName.toJson,
    "version" -> jsonVersion.parseJson
  ).compactPrint

  def trigger(executionId: Long, target: TargetExpr, frozenParameters: String, initiator: String): Future[Option[String]]

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

  override def trigger(executionId: Long, target: TargetExpr, frozenParameters: String, initiator: String): Future[Option[String]] = {
    logger.info(s"Hi, I'm $name! I will run operation #$executionId on behalf of: $initiator")
    Future.successful(None)
  }
}


class UnprocessableIntent extends IllegalArgumentException
