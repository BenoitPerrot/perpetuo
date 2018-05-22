package com.criteo.perpetuo.model

import com.criteo.perpetuo.util.json.ToJsonAlias
import com.twitter.inject.Test
import spray.json.JsString
import spray.json.JsonParser.ParsingException

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
    }.getMessage shouldBe "Expected to find `productName` at request root"
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
      "target" -> "*"
    )).compactPrint, "u.ser")
  }

}
