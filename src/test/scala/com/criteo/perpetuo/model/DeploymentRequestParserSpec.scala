package com.criteo.perpetuo.model

import com.criteo.perpetuo.util.json.ToJsonAlias
import com.twitter.inject.Test
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException
import spray.json._


class DeploymentRequestParserSpec extends Test {

  object TimeSink extends java.sql.Timestamp(0) {
    override def equals(other: java.sql.Timestamp): Boolean = true
  }

  test("DeploymentRequestParser rejects free text") {
    intercept[ParsingException] {
      DeploymentRequestParser.parse("free text", "")
    }
  }

  test("DeploymentRequestParser rejects JsObjects with invalid structure with a clear message") {
    intercept[ParsingException] {
      DeploymentRequestParser.parse(ToJsonAlias.deepToJson(Map(
        "foo" -> "bar"
      )).compactPrint, "")
    }.getMessage shouldBe "(deployment request): while required, no field named `productName` could be found"
  }

  test("DeploymentRequestParser parses deployment requests") {
    ProtoDeploymentRequest(
      "rejex",
      Version(JsString("1").compactPrint),
      Seq(ProtoDeploymentPlanStep("", JsString("*"), "")),
      "",
      "u.ser",
      TimeSink
    ) shouldBe DeploymentRequestParser.parse(ToJsonAlias.deepToJson(Map(
      "productName" -> "rejex",
      "version" -> "1",
      "plan" -> Seq(Map("target" -> "*"))
    )).compactPrint, "u.ser")
  }

  test("DeploymentRequestParser parses deployment plans") {
    DeploymentRequestParser.parse(ToJsonAlias.deepToJson(
      Map(
        "productName" -> "rejex",
        "version" -> "1",
        "plan" -> Seq(
          Map(
            "name" -> "Worldwide",
            "target" -> "*"
          )
        )
      )
    ).compactPrint, "").plan shouldBe Seq(
      ProtoDeploymentPlanStep("Worldwide", JsString("*"), "")
    )
  }

  test("DeploymentRequestParser loads and types correct targets") {
    foreach(
      ("foo".toJson, TargetWord("foo")),
      (Seq("foo", "bar").toJson, TargetUnion(Set(TargetWord("foo"), TargetWord("bar")))),
      (JsObject(), TargetTop),
      (Map("union" -> Seq("a", "b")).toJson, TargetUnion(Set(TargetWord("a"), TargetWord("b")))),
      (Map("union" -> Seq("a".toJson, JsObject())).toJson, TargetUnion(Set(TargetWord("a"), TargetTop))),
      (Map("intersection" -> Seq("a".toJson, Map("union" -> Seq(JsObject(), "b".toJson)).toJson)).toJson,
        TargetIntersection(Set(TargetWord("a"), TargetUnion(Set(TargetTop, TargetWord("b"))))))
    ) { case (input, output) =>
      DeploymentRequestParser.parseRootTargetExpression(input) shouldEqual output
    }
  }

  test("DeploymentRequestParser rejects incorrect targets") {
    foreach(
      (JsArray(), "Unexpected target element: []"),
      (60.toJson, "Unexpected target element: 60"),
      ("".toJson, "Unexpected target element: \"\""),
      (Seq(42).toJson, "Unexpected target element: 42"),
      (Seq("").toJson, "Unexpected target element: \"\""),
      (Seq(Seq("foo")).toJson, "Unexpected target element: [\"foo\"]"),
      (Map("k" -> Seq("abc")).toJson, "Unexpected target element: {\"k\":[\"abc\"]}"),
      (Map("a" -> Seq("abc").toJson, "b" -> JsObject()).toJson, "In target expressions, objects must contain at most one key; got the object {\"a\":[\"abc\"],\"b\":{}}"),
      (Map("union" -> "abc").toJson, "Unexpected target element: {\"union\":\"abc\"}"),
      (Map("intersection" -> 42).toJson, "Unexpected target element: {\"intersection\":42}"),
      (Map("union" -> Seq("a".toJson, Map("intersection" -> 42).toJson)).toJson, "Unexpected target element: {\"intersection\":42}")
    ) { case (input, errorMsg) =>
      the[ParsingException] thrownBy DeploymentRequestParser.parseRootTargetExpression(input) should have message errorMsg
    }
  }

  private def foreach[T](testCases: T*)(f: T => Unit): Unit =
    testCases
      .zipWithIndex
      .foreach { case (testCase, i) =>
        try {
          f(testCase)
        }
        catch {
          case e: Throwable => throw new AssertionError(s"Test case #${i + 1}: $e", e)
        }
      }
}
