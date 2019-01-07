package com.criteo.perpetuo.engine.executors

import com.twitter.inject.Test
import com.typesafe.config.ConfigFactory


class JenkinsTriggerSpec extends Test {
  test("JenkinsTrigger must be loadable by configuration, to be used with the singleExecutor dispatcher") {
    new JenkinsTrigger(ConfigFactory.parseString(
      """
        |name = "foo"
        |jobToken = "token"
        |host = "foo-01.criteo.com"
        |jobName = "deploy-foo"
      """.stripMargin))
  }
}
