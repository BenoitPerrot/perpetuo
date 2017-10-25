package com.criteo.perpetuo.engine

import com.criteo.perpetuo.engine.dispatchers.{TargetDispatcher, UnprocessableIntent}
import com.criteo.perpetuo.engine.invokers.{DummyInvoker, ExecutorInvoker}
import com.criteo.perpetuo.model.Version


class TargetDispatcherForTesting extends TargetDispatcher {

  private val executorInvoker = new DummyInvoker("invoker-for-testing")

  override def freezeParameters(productName: String, version: Version): String = {
    if (productName == TargetDispatcherForTesting.productWithNoDeployTypeName)
      throw UnprocessableIntent(s"$productName: product has no deploy type, while one is required")
    ""
  }

  override def dispatch(targetAtoms: Select, frozenParameters: String): Iterable[(ExecutorInvoker, Select)] =
    Map(executorInvoker -> targetAtoms)
}

object TargetDispatcherForTesting {
  val productWithNoDeployTypeName: String = "product-with-no-deploy-type"
}