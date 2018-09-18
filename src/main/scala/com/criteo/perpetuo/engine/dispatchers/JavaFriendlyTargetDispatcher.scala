package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Logging

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer with Logging {
  def get: TargetDispatcher = {
    val delegate = this

    new TargetDispatcher {
      override def freezeParameters(productName: String, version: Version): String =
        delegate.freezeParameters(productName, version)

      override def dispatch(targetExpr: TargetExpr, frozenParameters: String): Iterable[(ExecutionTrigger, TargetExpr)] =
        delegate.dispatch(targetExpr.asJava, frozenParameters).iterateAsScala.toIterable
    }
  }

  protected def dispatch(targetExpr: JavaSet[String], frozenParameters: String): JavaMap[ExecutionTrigger, JavaIterable[String]]
}
