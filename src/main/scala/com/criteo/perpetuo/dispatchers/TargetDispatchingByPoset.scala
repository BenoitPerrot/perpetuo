package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker


class TargetDispatchingByPoset(val executorsByPoset: ExecutorsByPoset) extends TargetDispatching {
  // todo? support multiple executors per target, possibly with the choice between AnyOf[ExecutorType] and AllOf[ExecutorType]

  override def assign(selectWord: String): Set[ExecutorInvoker] = executorsByPoset.getExecutors(selectWord)
}
