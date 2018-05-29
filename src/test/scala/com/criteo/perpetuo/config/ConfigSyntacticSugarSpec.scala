package com.criteo.perpetuo.config

import com.twitter.inject.Test
import com.typesafe.config.{Config, ConfigException, ConfigFactory}


/**
  * It checks that each added method .tryGetFoo() behaves like the built-in .getFoo(),
  * except that it first checks if the path exists...
  *
  * It's basically a guard to prevent anyone from trying to reimplement the syntactic sugar
  * in a clever way :p because a few days have already been wasted in many attempts to do
  * type checking in Scala.
  */
class ConfigSyntacticSugarSpec extends Test {

  import com.criteo.perpetuo.config.ConfigSyntacticSugar._

  private val config = ConfigFactory.parseString(
    """
      |foo {
      |  str = "description"
      |  nb = 42
      |  strList = [
      |    "abc",
      |    "def"
      |  ]
      |}
      |bar {
      |  list = [
      |    "abc",
      |    42
      |  ],
      |  configs = [
      |    {
      |      config {
      |        baz = true
      |      }
      |    }
      |  ]
      |}
    """.stripMargin)

  test("Try-getters succeed on values of the expected type") {
    val foobar = config.tryGetConfigList("bar.configs")
    foobar shouldBe defined
    foobar.get.foreach(_ shouldBe a[Config])
    val bar = new ConfigSyntacticSugar(foobar.get.head).tryGetConfig("config")
    bar shouldBe defined
    bar.get shouldEqual ConfigFactory.parseString("baz = true")

    config.tryGetStringList("foo.strList") shouldEqual Some(Seq("abc", "def"))
    config.tryGetString("foo.str") shouldEqual Some("description")
    config.getIntOrElse("foo.nb", 51) shouldEqual 42
  }

  test("Try-getters succeed on converting types as documented by typesafe") {
    config.tryGetStringList("bar.list") shouldEqual Some(Seq("abc", "42"))
    config.tryGetString("foo.nb") shouldEqual Some("42")
  }

  test("Try-getters succeed on missing values") {
    config.tryGetConfig("a") shouldBe None
    config.tryGetConfigList("a") shouldBe None
    config.tryGetStringList("a") shouldBe None
    config.tryGetString("a") shouldBe None
    config.getIntOrElse("a", 51) shouldEqual 51
  }

  test("Trying to get a value of an unexpected type fails with a helpful error message") {
    a[ConfigException.WrongType] shouldBe thrownBy(config.tryGetConfigList("foo.strList"))
    a[ConfigException.WrongType] shouldBe thrownBy(config.tryGetConfig("foo.nb"))
    a[ConfigException.WrongType] shouldBe thrownBy(config.tryGetStringList("foo.nb"))
    a[ConfigException.WrongType] shouldBe thrownBy(config.getIntOrElse("foo.str", 7))
  }
}
