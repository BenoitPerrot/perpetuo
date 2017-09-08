package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.executors.ExecutorInvoker

import scala.collection.JavaConverters._


case class SingleTargetDispatcher(executorInvoker: ExecutorInvoker) extends TargetDispatcher {
  override def assign(selectWord: String): java.lang.Iterable[ExecutorInvoker] = Seq(executorInvoker).asJava
}
