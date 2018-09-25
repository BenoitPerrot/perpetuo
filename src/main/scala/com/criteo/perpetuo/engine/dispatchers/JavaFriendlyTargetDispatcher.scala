package com.criteo.perpetuo.engine.dispatchers

import java.util.{Map => JavaMap}

import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model._
import com.twitter.inject.Logging

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer with Logging {
  def get: TargetDispatcher = {
    val delegate = this

    new TargetDispatcher {
      override def freezeParameters(productName: String, version: Version): String =
        delegate.freezeParameters(productName, version)

      protected override def dispatch(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] = {
        delegate.dispatch(targetExpr, frozenParameters)
          .entrySet.iterator.asScala
          .map(entry => (entry.getKey, entry.getValue))
          .toIterable
      }
    }
  }

  protected def dispatch(targetExpr: TargetExpr, frozenParameters: String): JavaMap[ExecutionTrigger, TargetExpr]
}
