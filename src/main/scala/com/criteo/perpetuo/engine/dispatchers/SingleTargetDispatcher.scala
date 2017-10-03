package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.model.Version

import scala.collection.JavaConverters._


case class SingleTargetDispatcher(executorInvoker: ExecutorInvoker) extends TargetDispatcher {
  override def freezeParameters(executionKind: String, productName: String, version: Version) = ""

  override def dispatch(targetAtoms: JavaIterable[String], frozenParameters: String): JavaMap[ExecutorInvoker, JavaIterable[String]] =
    Map(executorInvoker -> targetAtoms).asJava
}
