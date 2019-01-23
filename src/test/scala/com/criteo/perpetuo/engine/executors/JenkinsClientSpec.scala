package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.config.TestConfig
import com.twitter.inject.Test

class JenkinsClientSpec extends Test {

  private val config = TestConfig.executorConfig("jenkins")

  val client = new JenkinsClient("localhost", config.tryGetInt("port"), config.tryGetBoolean("ssl"), config.tryGetString("username"), config.tryGetString("password"))

  test("URL with queries is correctly formatted") {
    client.apiPath("job/myjob", Map("key1" -> "value1", "with space" -> "val")) shouldBe "/job/myjob?key1=value1&with+space=val"
  }

  test("URL with token is correctly formatted") {
    client.apiPath("job/myjob", Map("token" -> "token", "key1" -> "value1")) shouldBe "/job/myjob?token=token&key1=value1"
  }

  test("URL without token nor queries is correctly formatted") {
    client.apiPath("job/myjob") shouldBe "/job/myjob"
  }
}
