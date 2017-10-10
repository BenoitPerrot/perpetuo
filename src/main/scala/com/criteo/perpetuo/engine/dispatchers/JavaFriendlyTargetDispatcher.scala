package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap, Set => JavaSet}

import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.model.Version

import scala.collection.JavaConverters._


abstract class JavaFriendlyTargetResolver extends Provider[TargetResolver] {
  def get: TargetResolver = {
    val delegate = this

    new TargetResolver {
      override def toAtoms(productName: String, productVersion: String, targetWord: String): Iterable[String] =
        delegate.fromTargetWordToAtoms(productName, productVersion, targetWord).asScala
    }
  }

  protected def fromTargetWordToAtoms(productName: String, productVersion: String, targetWord: String): JavaIterable[String]
}


abstract class JavaFriendlyTargetDispatcher extends Provider[TargetDispatcher] with ParameterFreezer {
  def get: TargetDispatcher = {
    val delegate = this

    new TargetDispatcher {
      override def freezeParameters(executionKind: String, productName: String, version: Version): String =
        delegate.freezeParameters(executionKind, productName, version)

      override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)] =
        delegate.dispatch(targetAtoms.asJava, frozenParameters).asScala.mapValues(_.asScala.toSet)
    }
  }

  protected def dispatch(targetAtoms: JavaSet[String], frozenParameters: String): JavaMap[ExecutorInvoker, JavaIterable[String]]
}
