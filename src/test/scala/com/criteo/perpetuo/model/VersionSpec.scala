package com.criteo.perpetuo.model

import com.twitter.inject.Test
import spray.json._


class VersionSpec extends Test {
  private val padded5 = "0000000000005"
  private val padded42 = "0000000000042"
  private val padded420 = "0000000000420"
  private val filled = "1494242421234"

  private def convert(value: String): String =
    Version(value).value.parseJson.asInstanceOf[JsArray].elements.head.asJsObject.fields("value").asJsObject.fields("main").asInstanceOf[JsString].value

  "Self check" should {
    "assert that the encoded number size is the size of a timestamp in ms" in {
      convert("42").length shouldEqual System.currentTimeMillis.toString.length
    }
  }

  "Creating a Version" should {
    "take the input as-is if it contains too long numbers" in {
      convert("abc-12345678901234-def") shouldEqual "abc-12345678901234-def"
    }

    "take the input as-is if it contains too many numbers" in {
      convert("a1b2c3d4e5") shouldEqual "a1b2c3d4e5"
    }

    "accept a sha-1" in {
      // it's already covered by tests above, but it makes the idea clear
      convert("b19cd1527b508127949da6c2861617b0c978ce1f") shouldEqual "b19cd1527b508127949da6c2861617b0c978ce1f"
    }
  }

  "Versions numbers" should {
    "be encoded with leading zeros in front of all numbers" in {
      convert("5") shouldEqual padded5
      convert("42") shouldEqual padded42
      convert("420") shouldEqual padded420
      convert(filled) shouldEqual filled
      convert("foo42") shouldEqual s"foo$padded42"
      convert("42bar") shouldEqual s"${padded42}bar"
      convert("foo42bar") shouldEqual s"foo${padded42}bar"
      convert("foo42bar5") shouldEqual s"foo${padded42}bar$padded5"
      convert(s"foo${filled}bar") shouldEqual s"foo${filled}bar"
    }
  }

  "Versions" should {
    "support equality on numbers" in {
      convert("042") shouldEqual convert("42")
      convert("42") shouldNot equal(convert("420"))
      convert("42") shouldNot equal(convert("51"))
    }

    "be sortable by Slick wrt each number node" in {
      // the attribute `value` is used to sort records on SQL side
      Seq(
        "5.3",
        "05.1",
        "5",
        "42",
        "101",
        "foo042bar",
        "42-6",
        "foo42bar",
        "5.21.1",
        "420",
        "042",
        "foo04.02.baz",
        "foo04.2.bar",
        "42.51",
        "51.2",
        "a1b2c3d",
        "a1b1c3d",
        "a1b2c3a",
        "5.21"
      ).map(convert).sorted.map(Version(_).structured.head.value.head._2) shouldEqual Seq(
        "5",
        "5.1",
        "5.3",
        "5.21",
        "5.21.1",
        "42",
        "42",
        "42-6",
        "42.51",
        "51.2",
        "101",
        "420",
        "a1b1c3d",
        "a1b2c3a",
        "a1b2c3d",
        "foo4.2.bar",
        "foo4.2.baz",
        "foo42bar",
        "foo42bar"
      )
    }
  }
}
