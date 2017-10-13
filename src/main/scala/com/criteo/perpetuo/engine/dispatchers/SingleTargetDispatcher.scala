package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.Select
import com.criteo.perpetuo.engine.invokers.ExecutorInvoker
import com.criteo.perpetuo.model.Version


case class SingleTargetDispatcher(executorInvoker: ExecutorInvoker) extends TargetDispatcher {
  override def freezeParameters(executionKind: String, productName: String, version: Version) = ""

  override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)] =
    Map(executorInvoker -> targetAtoms)
}
