package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.Select
import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model.Version


case class SingleTargetDispatcher(executionTrigger: ExecutionTrigger) extends TargetDispatcher {
  override def freezeParameters(productName: String, version: Version): String = ""

  override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutionTrigger, Select)] =
    Map(executionTrigger -> targetAtoms)
}
