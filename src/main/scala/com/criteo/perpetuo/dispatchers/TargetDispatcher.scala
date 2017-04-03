package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.app.AppConfig
import com.criteo.perpetuo.executors.ExecutorInvoker


trait TargetDispatcher {
  def assign(selectWord: String): Set[ExecutorInvoker]
}


object TargetDispatcher {
  lazy val fromConfig: TargetDispatcher = fromName(AppConfig.get("targetDispatcher"))
  def fromName(objectName: String): TargetDispatcher = {
    val cls = Class.forName(objectName + "$")
    cls.getField("MODULE$").get(cls).asInstanceOf[TargetDispatcher]
  }
}
