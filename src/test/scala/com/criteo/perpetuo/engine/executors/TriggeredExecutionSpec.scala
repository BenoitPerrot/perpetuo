package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.{AppConfig, PluginLoader}
import com.criteo.perpetuo.model.{ExecutionState, ShallowExecutionTrace}
import com.twitter.inject.Test


class TriggeredExecutionSpec extends Test {
  val finder = new TriggeredExecutionFinder(AppConfig, new PluginLoader(null, AppConfig))

  test("The TriggeredExecutionFinder can load a Rundeck execution") {
    finder(ShallowExecutionTrace(42, "rundeck", Some("https://rundeck.somewhere/project/foo-bar/execution/show/42"), ExecutionState.completed, "")).stopper shouldBe defined
  }

  test("The TriggeredExecutionFinder fails to load an execution of an unknown type") {
    val exc = the[RuntimeException] thrownBy finder(ShallowExecutionTrace(51, "idont-kn.ow", Some("https://idont-kn.ow/what.im?doing"), ExecutionState.running, ""))
    exc.getMessage should startWith("Could not find")
  }

  test("The TriggeredExecutionFinder fails to load an execution that has no href") {
    val exc = the[RuntimeException] thrownBy finder(ShallowExecutionTrace(51, "rundeck", None, ExecutionState.running, ""))
    exc.getMessage should startWith("No href")
  }
}
