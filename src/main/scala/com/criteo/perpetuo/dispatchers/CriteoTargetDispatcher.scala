package com.criteo.perpetuo.dispatchers

import com.criteo.perpetuo.app.AppConfig
import com.criteo.perpetuo.executors.{DummyInvoker, RundeckInvoker}


object CriteoTargetDispatcher extends {

  private val ALL = "*" // could be "toto" actually, it has no impact at all until we have multiple executors :)

} with TargetDispatcherByPoset(
  new ExecutorsByPoset(

    // todo: Rundeck is not yet registered in Consul, so we hard-code all the hostnames for now.
    // todo: for now, we only have one active node per environment...
    executorMap = AppConfig.env match {
      case "test" => Map(
        ALL -> new DummyInvoker("test invoker")
      )
      case "local" => Map(
        ALL -> new RundeckInvoker("localhost", 4440, "rundeck")
      )
      case env => Map(
        ALL -> new RundeckInvoker(s"rundeck.central.criteo.$env", 443, "rundeck")
      )
    },

    getParents = (_) => Seq.empty

  )
)
