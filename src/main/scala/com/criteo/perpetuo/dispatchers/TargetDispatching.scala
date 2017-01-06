package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.dao.enums.Operation.Operation
import com.criteo.perpetuo.executors.ExecutorInvoker
import com.twitter.util.Future


trait TargetDispatching {
  type Invocation = (Operation) => Option[Future[String]]

  def toInvocations(tactics: Tactics, select: Select): Iterator[Invocation] = dispatch(select).map {
    case (executor, dispatchedSelect) => executor.trigger(_, tactics, dispatchedSelect)
  }

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
