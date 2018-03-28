package com.criteo.perpetuo.engine.dispatchers

import com.criteo.perpetuo.engine.Select
import com.criteo.perpetuo.engine.executors.{DummyExecutionTrigger, ExecutionTrigger}
import com.criteo.perpetuo.model.Version


class TargetDispatcherForTesting extends TargetDispatcher {
  private val executionTrigger = new DummyExecutionTrigger("trigger-for-testing")

  override def freezeParameters(productName: String, version: Version): String = {
    if (productName == TargetDispatcherForTesting.productWithNoDeployTypeName)
      throw UnprocessableIntent(s"$productName: product has no deploy type, while one is required")
    ""
  }

  override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutionTrigger, Select)] =
    Map(executionTrigger -> targetAtoms)
}

object TargetDispatcherForTesting {
  val productWithNoDeployTypeName: String = "product-with-no-deploy-type"
}
