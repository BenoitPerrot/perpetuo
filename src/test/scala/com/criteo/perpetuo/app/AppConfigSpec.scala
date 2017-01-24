package com.criteo.perpetuo.app

import com.twitter.inject.Test
import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.ConfigFactory


class AppConfigSpec extends Test {
  val example = new LoadedRootAppConfig(ConfigFactory.parseString(
    """
    toto: "foo"
    env: "preprod"
    other1: "abc"
    preprod: {
      toto: ${env}
      other2: "def"
    }
    int: {
      toto: 42
      test: {
        toto: 51
        other3: 9
      }
    }
    """
  ))


  "AppConfig" should {
    "expose the environment" in {
      example.env shouldEqual "preprod"
    }
    "expose a generic getter on any value" in {
      example.get[Int]("int.toto") shouldEqual 42
      example.get[String]("other1") shouldEqual "abc"
    }
    "throw when configuration doesn't not match expectation" in {
      an[Missing] shouldBe thrownBy { example.get[Int]("abc") }
      an[ClassCastException] shouldBe thrownBy { example.get[Int]("other1") }
      an[Missing] shouldBe thrownBy { example.get[Int]("int.tutu") }
    }
    "expose a method to get a sub-configuration" in {
      val subConfig = example.under("int")
      subConfig.isInstanceOf[AppConfig] should be // YodaSpec syntax ;)
      subConfig.get[Int]("toto") shouldEqual 42
    }
    "make the environment level implicit at both ends of a path" in {
      example.get[String]("toto") shouldEqual "preprod"
      example.get[String]("other2") shouldEqual "def"
    }
    "doesn't mix up paths with same base names" in {
      example.get[AnyRef]("int.toto") shouldEqual 42
      example.get[AnyRef]("toto") shouldEqual "preprod"
    }
    "allow to override the environment at the root, being applicable in the whole tree" in {
      example.withEnv("test").under("int").get[Int]("toto") shouldEqual 51
      example.withEnv("test").under("int").get[Int]("other3") shouldEqual 9
    }
    "still support direct access to a path disregarding environment-related alternatives" in {
      example.withEnv("test").get[Int]("int.toto") shouldEqual 42
      example.get[String]("preprod.toto") shouldEqual "preprod"
      example.get[Int]("int.test.other3") shouldEqual 9
      example.under("int.test").get[Int]("toto") shouldEqual 51
      a[Missing] shouldBe thrownBy { example.get[String]("int.other3") }
    }
    "make overriding environment impact the values when applicable" in {
      example.withEnv("test").get[String]("preprod.toto") shouldEqual "test"
    }
  }
}
