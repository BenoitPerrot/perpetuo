package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker


trait TargetDispatching {
  /**
    * the abstract method to define in subclasses: the dispatcher
    */
  def assign(selectWord: String): Set[ExecutorInvoker]
}


object TargetDispatching {
  def fromName(objectName: String): TargetDispatching = {
    val cls = Class.forName(objectName + "$")
    cls.getField("MODULE$").get(cls).asInstanceOf[TargetDispatching]
  }
}
