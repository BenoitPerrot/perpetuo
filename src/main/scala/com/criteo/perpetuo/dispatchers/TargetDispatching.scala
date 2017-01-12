package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker


trait TargetDispatching {
  /**
    * the abstract method to define in subclasses: the dispatcher
    */
  def dispatch(select: Select): Iterator[(ExecutorInvoker, Select)]
}


object TargetDispatching {
  def fromName(objectName: String): TargetDispatching = {
    val cls = Class.forName(objectName + "$")
    cls.getField("MODULE$").get(cls).asInstanceOf[TargetDispatching]
  }
}
