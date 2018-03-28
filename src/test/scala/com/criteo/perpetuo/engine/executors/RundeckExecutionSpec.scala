package com.criteo.perpetuo.engine.executors

import com.twitter.inject.Test

class RundeckExecutionSpec extends Test {
  test("A RundeckExecution is built from a the permalink returned by Rundeck on trigger") {
    val remote = new RundeckExecution("https://rundeck.criteo/project/my-project/executcriteon/show/42")
    remote.host shouldEqual "rundeck.criteo"
    remote.executionNumber shouldEqual 42

    val local = new RundeckExecution("http://localhost:4440/project/my-project/execution/show/51")
    local.host shouldEqual "localhost"
    local.executionNumber shouldEqual 51
  }

  test("If a log href makes no sense to the assigned execution factory, the instantiation must behave appropriately") {
    val exc = the[IllegalArgumentException] thrownBy new RundeckExecution("http://rundeck.criteo/menu/home")
    exc.getMessage shouldEqual "Cannot find a proper Rundeck executor from http://rundeck.criteo/menu/home"
  }
}
