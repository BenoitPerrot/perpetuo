package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker


trait TargetDispatcher {
  def assign(selectWord: String): Set[ExecutorInvoker]
}
