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
      (Seq("foo", "bar").toJson, TargetUnion(Set(TargetWord("foo"), TargetWord("bar"))))
    ).foreach { case (input, output) =>
      DeploymentRequestParser.parseTargetExpression(input) shouldEqual output
    }
  }

  test("DeploymentRequestParser rejects incorrect targets") {
    Seq(
      (JsArray(), "Expected `target` to be a non-empty JSON array or string, got: []"),
      (JsObject(), "Expected `target` to be a non-empty JSON array or string, got: {}"),
      (60.toJson, "Expected `target` to be a non-empty JSON array or string, got: 60"),
      ("".toJson, "Expected `target` to be a non-empty JSON array or string, got: \"\""),
      (Seq(42).toJson, "Expected a non-empty JSON string in the `target` array, got: 42"),
      (Seq("").toJson, "Expected a non-empty JSON string in the `target` array, got: \"\"")
    ).foreach { case (input, errorMsg) =>
      the[ParsingException] thrownBy DeploymentRequestParser.parseTargetExpression(input) should
        have message errorMsg
    }
  }
}
