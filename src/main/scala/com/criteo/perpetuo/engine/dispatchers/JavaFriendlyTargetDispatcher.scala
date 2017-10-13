package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

import com.criteo.perpetuo.engine._
import com.criteo.perpetuo.engine.invokers.ExecutorInvoker
import com.criteo.perpetuo.model.Version

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer {
  def get: TargetDispatcher = {
    val delegate = this

    new TargetDispatcher {
      override def freezeParameters(executionKind: String, productName: String, version: Version): String =
        delegate.freezeParameters(executionKind, productName, version)

      override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)] =
        delegate.dispatch(targetAtoms.asJava, frozenParameters).iterateAsScala.toIterable
    }
  }

  protected def dispatch(targetAtoms: JavaSet[String], frozenParameters: String): JavaMap[ExecutorInvoker, JavaIterable[String]]
}
