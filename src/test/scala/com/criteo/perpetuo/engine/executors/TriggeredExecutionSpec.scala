package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.PluginLoader
import com.criteo.perpetuo.model.{ExecutionState, ShallowExecutionTrace}
import com.twitter.inject.Test


class TriggeredExecutionSpec extends Test {
  val finder = new TriggeredExecutionFinder(new PluginLoader(null))

  test("The TriggeredExecutionFinder can load a Rundeck execution") {
    finder(ShallowExecutionTrace(42, Some("https://rundeck.somewhere/project/foo-bar/execution/show/42"), ExecutionState.completed, ""), "rundeck").stopper shouldBe defined
  }

  test("The TriggeredExecutionFinder fails to load an execution of an unknown type") {
    val exc = the[RuntimeException] thrownBy finder(ShallowExecutionTrace(51, Some("https://idont-kn.ow/what.im?doing"), ExecutionState.running, ""), "unknown")
    exc.getMessage should startWith("Could not find")
  }

  test("The TriggeredExecutionFinder fails to load an execution that has no href") {
    val exc = the[RuntimeException] thrownBy finder(ShallowExecutionTrace(51, None, ExecutionState.running, ""), "")
    exc.getMessage should startWith("No href")
  }
}
