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
    Seq(
      ("foo".toJson, TargetWord("foo")),
      (Seq("foo", "bar").toJson, TargetUnion(Set(TargetWord("foo"), TargetWord("bar")))),
      (Map("union" -> Seq("a", "b")).toJson, TargetUnion(Set(TargetWord("a"), TargetWord("b"))))
    ).foreach { case (input, output) =>
      DeploymentRequestParser.parseRootTargetExpression(input) shouldEqual output
    }
  }

  test("DeploymentRequestParser rejects incorrect targets") {
    Seq(
      (JsArray(), "Unexpected element in the target expression: []"),
      (JsObject(), "In target expressions, objects must contain exactly one key (`union`); got the object {}"),
      (60.toJson, "Unexpected element in the target expression: 60"),
      ("".toJson, "Unexpected element in the target expression: \"\""),
      (Seq(42).toJson, "Unexpected element in the target expression: 42"),
      (Seq("").toJson, "Unexpected element in the target expression: \"\""),
      (Seq(Seq("foo")).toJson, "Unexpected element in the target expression: [\"foo\"]"),
      (Map("k" -> Seq("abc")).toJson, "In target expressions, objects must have `union` as key and an array as value; got: {\"k\":[\"abc\"]}"),
      (Map("union" -> "abc").toJson, "In target expressions, objects must have `union` as key and an array as value; got: {\"union\":\"abc\"}")
    ).foreach { case (input, errorMsg) =>
      the[ParsingException] thrownBy DeploymentRequestParser.parseRootTargetExpression(input) should
        have message errorMsg
    }
  }
}
