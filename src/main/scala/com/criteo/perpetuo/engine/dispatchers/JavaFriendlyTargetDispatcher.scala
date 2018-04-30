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

      override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutionTrigger, Select)] =
        delegate.dispatch(targetAtoms.asJava, frozenParameters).iterateAsScala.toIterable
    }
  }

  protected def dispatch(targetAtoms: JavaSet[String], frozenParameters: String): JavaMap[ExecutionTrigger, JavaIterable[String]]
}
