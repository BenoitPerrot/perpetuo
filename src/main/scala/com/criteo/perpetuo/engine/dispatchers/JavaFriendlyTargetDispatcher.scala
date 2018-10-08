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

      protected override def dispatch(targetAtoms: Set[TargetAtom], frozenParameters: String): Iterable[(ExecutionTrigger, Set[TargetAtom])] =
        delegate.dispatch(targetAtoms, frozenParameters)
          .entrySet.iterator.asScala
          .map(entry => (entry.getKey, entry.getValue))
          .toIterable
    }
  }

  protected def dispatch(targetAtoms: Set[TargetAtom], frozenParameters: String): JavaMap[ExecutionTrigger, Set[TargetAtom]]
}
