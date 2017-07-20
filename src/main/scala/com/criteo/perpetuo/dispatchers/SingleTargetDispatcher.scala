package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.engine.executors.{DummyInvoker, ExecutorInvoker}
import scala.collection.JavaConverters._


case class SingleTargetDispatcher(executorInvoker: ExecutorInvoker) extends TargetDispatcher {
  override def assign(selectWord: String): java.lang.Iterable[ExecutorInvoker] = Seq(executorInvoker).asJava
}


object DummyTargetDispatcher extends SingleTargetDispatcher(executorInvoker = new DummyInvoker("Default Dummy Invoker"))
