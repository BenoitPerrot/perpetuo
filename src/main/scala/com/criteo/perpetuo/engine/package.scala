package com.criteo.perpetuo

import com.criteo.perpetuo.engine.executors.ExecutionTrigger
import com.criteo.perpetuo.model._


package object engine {

  trait Provider[T] {
    def get: T
  }

  type ExecutionsToTrigger = Iterable[(Long, Version, TargetAtomSet, ExecutionTrigger)]
  type SpecAndInvocations = Iterable[(ExecutionSpecification, Vector[(ExecutionTrigger, TargetAtomSet)])]
  type OperationCreationParams = (Operation.Kind, SpecAndInvocations)

}
