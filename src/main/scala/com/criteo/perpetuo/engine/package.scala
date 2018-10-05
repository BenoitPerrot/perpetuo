package com.criteo.perpetuo

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model._


package object engine {

  trait Provider[T] {
    def get: T
  }

  type ExecutionsToTrigger = Iterable[(Long, Version, TargetExpr, ExecutionTrigger)]
  type OperationCreationParams = (Operation.Kind, Iterable[(ExecutionSpecification, Vector[(ExecutionTrigger, TargetExpr)])], Option[TargetAtomSet])

}
