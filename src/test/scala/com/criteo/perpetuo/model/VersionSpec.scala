package com.criteo.perpetuo.model

import com.twitter.inject.Test


class VersionSpec extends Test {
  private val padded5 = "0000000000005"
  private val padded42 = "0000000000042"
  private val padded420 = "0000000000420"
  private val filled = "1494242421234"

  "Self check" should {
    "assert that the encoded number size is the size of a timestamp in ms" in {
      Version("42").value.length shouldEqual System.currentTimeMillis.toString.length
    }
  }

  "Creating a Version" should {
    "be forbidden if the representation contains too long numbers" in {
      an[IllegalArgumentException] shouldBe thrownBy(Version("12345678901234"))
    }
  }

  "Versions numbers" should {
    "be encoded with leading zeros in front of all numbers" in {
      Version("5").value shouldEqual padded5
      Version("42").value shouldEqual padded42
      Version("420").value shouldEqual padded420
      Version(filled).value shouldEqual filled
      Version("foo42").value shouldEqual s"foo$padded42"
      Version("42bar").value shouldEqual s"${padded42}bar"
      Version("foo42bar").value shouldEqual s"foo${padded42}bar"
      Version("foo42bar5").value shouldEqual s"foo${padded42}bar$padded5"
      Version(s"foo${filled}bar").value shouldEqual s"foo${filled}bar"
    }
  }

  "Versions" should {
    "support equality on numbers" in {
      Version("042") shouldEqual Version("42")
      Version("42") shouldNot equal(Version("420"))
      Version("42") shouldNot equal(Version("51"))
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
      ).map(Version(_).value).sorted.map(Version(_).toString) shouldEqual Seq(
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
