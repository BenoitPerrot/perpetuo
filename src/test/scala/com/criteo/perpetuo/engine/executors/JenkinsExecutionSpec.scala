package com.criteo.perpetuo.engine.executors

import com.twitter.inject.Test


class JenkinsExecutionSpec extends Test {

  test("Execution's host, job name and build id are correctly parsed") {
    val execution = new JenkinsExecution("http://build.criteois.lan/job/test/17/")
    execution.host shouldEqual "build.criteois.lan"
    execution.jobName shouldEqual "test"
    execution.buildId shouldEqual "17"

    val execution2 = new JenkinsExecution("http://build.criteois.lan/job/test-executor/18")
    execution2.host shouldEqual "build.criteois.lan"
    execution2.jobName shouldEqual "test-executor"
    execution2.buildId shouldEqual "18"

    val localExecution = new JenkinsExecution("http://localhost:8080/job/perpetuo-executor/1")
    localExecution.host shouldEqual "localhost"
    localExecution.jobName shouldEqual "perpetuo-executor"
    localExecution.buildId shouldEqual "1"
  }

  test("Wrong href generates an error") {
    val exc = the[IllegalArgumentException] thrownBy new JenkinsExecution("http://build.criteois.lan/job/test")
    exc.getMessage shouldEqual "Cannot find a proper Jenkins executor from http://build.criteois.lan/job/test"
  }
}
