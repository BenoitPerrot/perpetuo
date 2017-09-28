package com.criteo.perpetuo.config

import com.twitter.inject.Test
import com.typesafe.config.ConfigException.Missing
import com.typesafe.config.{Config, ConfigFactory}


class AppConfigSpec extends Test {
  private val example: Config = ConfigFactory.parseString(
    """
    toto: "foo"
    other1: "abc"
    int: {
      toto: 42
      test: {
        toto: 51
        other3: 9
      }
    }
    """
  )

  private val preprodConfig: AppConfig = new AppConfig(example)
  private val testConfig: AppConfig = new AppConfig(example)


  "AppConfig" should {
    "expose a generic getter on any value" in {
      preprodConfig.get[Int]("int.toto") shouldEqual 42
      preprodConfig.get[String]("other1") shouldEqual "abc"
    }
    "throw when configuration doesn't match expectation" in {
      a[Missing] shouldBe thrownBy { preprodConfig.get[Int]("abc") }
      a[ClassCastException] shouldBe thrownBy { preprodConfig.get[Int]("other1") }
      a[Missing] shouldBe thrownBy { preprodConfig.get[Int]("int.tutu") }
    }
    "expose a method to get a sub-configuration" in {
      val subConfig = preprodConfig.under("int")
      subConfig.isInstanceOf[AppConfig] should be // YodaSpec syntax ;)
      subConfig.get[Int]("toto") shouldEqual 42
    }
    "doesn't mix up paths with same base names" in {
      preprodConfig.get[AnyRef]("int.toto") shouldEqual 42
    }
    "still support direct access to a path disregarding environment-related alternatives" in {
      testConfig.get[Int]("int.toto") shouldEqual 42
      preprodConfig.get[Int]("int.test.other3") shouldEqual 9
      preprodConfig.under("int.test").get[Int]("toto") shouldEqual 51
      a[Missing] shouldBe thrownBy { preprodConfig.get[String]("int.other3") }
    }
  }
}
