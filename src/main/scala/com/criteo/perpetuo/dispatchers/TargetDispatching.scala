package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.executors.ExecutorInvoker
import com.typesafe.config.ConfigFactory


trait TargetDispatching {
  /**
    * the abstract method to define in subclasses: the dispatcher
    */
  def assign(selectWord: String): Set[ExecutorInvoker]
}


object TargetDispatching {
  private val config = ConfigFactory.load()
  lazy val fromConfig: TargetDispatching = fromName(config.getConfig("targetDispatcher").getString(config.getString("env")))
  def fromName(objectName: String): TargetDispatching = {
    val cls = Class.forName(objectName + "$")
    cls.getField("MODULE$").get(cls).asInstanceOf[TargetDispatching]
  }
}
