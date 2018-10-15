package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

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
        delegate.dispatch(targetAtoms.map(_.name).asJava, frozenParameters)
          .entrySet.iterator.asScala
          .map(entry => (entry.getKey, entry.getValue.iterator().asScala.map(TargetAtom).toSet)).toIterable
    }
  }

  protected def dispatch(targetAtoms: JavaSet[String], frozenParameters: String): JavaMap[ExecutionTrigger, JavaIterable[String]]
}
