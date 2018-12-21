package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.AppConfig
import com.twitter.inject.Test

class JenkinsClientSpec extends Test {

  private val jenkinsConfig = AppConfig.executorConfig("jenkins")

  test("URL with queries is correctly formatted") {
    val client = new JenkinsClient(jenkinsConfig, "localhost")
    client.apiPath("job/myjob", Map("key1" -> "value1", "with space" -> "val")) shouldBe "/job/myjob?key1=value1&with+space=val"
  }

  test("URL with token is correctly formatted") {
    val client = new JenkinsClient(jenkinsConfig,"localhost")
    client.apiPath("job/myjob", Map("token" -> "token", "key1" -> "value1")) shouldBe "/job/myjob?token=token&key1=value1"
  }

  test("URL without token nor queries is correctly formatted") {
    val client = new JenkinsClient(jenkinsConfig, "localhost")
    client.apiPath("job/myjob") shouldBe "/job/myjob"
  }
}