package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}


case class SingleTargetDispatcher(executorInvoker: ExecutorInvoker) extends TargetDispatching {
  override def assign(selectWord: String): Set[ExecutorInvoker] = Set(executorInvoker)
}


object DummyTargetDispatcher extends SingleTargetDispatcher(executorInvoker = new DummyInvoker("Default Dummy Invoker"))
