package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker


class TargetDispatcherByPoset(val executorsByPoset: ExecutorsByPoset) extends TargetDispatcher {
  // todo? support multiple executors per target, possibly with the choice between AnyOf[ExecutorType] and AllOf[ExecutorType]

  override def assign(selectWord: String): Set[ExecutorInvoker] = executorsByPoset.getExecutors(selectWord)
}
