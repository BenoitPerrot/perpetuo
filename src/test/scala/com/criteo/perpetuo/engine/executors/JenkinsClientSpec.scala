package com.criteo.perpetuo.engine.executors

import com.twitter.inject.Test

class JenkinsClientSpec extends Test {

  test("URL with queries is correctly formatted") {
    val client = new JenkinsClient("localhost")
    client.apiPath("job/myjob", Map("key1" -> "value1", "with space" -> "val")) shouldBe "/job/myjob?key1=value1&with+space=val"
  }

  test("URL with token is correctly formatted") {
    val client = new JenkinsClient("localhost")
    client.apiPath("job/myjob", Map("token" -> "token", "key1" -> "value1")) shouldBe "/job/myjob?token=token&key1=value1"
  }

  test("URL without token nor queries is correctly formatted") {
    val client = new JenkinsClient("localhost")
    client.apiPath("job/myjob") shouldBe "/job/myjob"
  }
}