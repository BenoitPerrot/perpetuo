package com.criteo.perpetuo.engine.dispatchers

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import com.criteo.perpetuo.engine.executors.ExecutorInvoker

import scala.collection.JavaConverters._


case class SingleTargetDispatcher(executorInvoker: ExecutorInvoker) extends TargetDispatcher {
  override def dispatch(targetAtoms: JavaIterable[String]): JavaMap[ExecutorInvoker, JavaIterable[String]] =
    Map(executorInvoker -> targetAtoms).asJava
}
