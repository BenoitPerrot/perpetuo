package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.app.AppConfig
import com.criteo.perpetuo.executors.ExecutorInvoker


trait TargetDispatching {
  /**
    * the abstract method to define in subclasses: the dispatcher
    */
  def assign(selectWord: String): Set[ExecutorInvoker]
}


object TargetDispatching {
  lazy val fromConfig: TargetDispatching = fromName(AppConfig.get[String]("targetDispatcher"))
  def fromName(objectName: String): TargetDispatching = {
    val cls = Class.forName(objectName + "$")
    cls.getField("MODULE$").get(cls).asInstanceOf[TargetDispatching]
  }
}
