package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.{DummyInvoker, ExecutorInvoker}


case class SingleTargetDispatcher(executorInvoker: ExecutorInvoker) extends TargetDispatching {
  override def dispatch(select: Select): Iterator[(ExecutorInvoker, Select)] =
    Iterator((executorInvoker, select))
}


object DummyTargetDispatcher extends SingleTargetDispatcher(executorInvoker = new DummyInvoker("Default Dummy Invoker"))
