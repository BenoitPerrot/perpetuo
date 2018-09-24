package com.criteo.perpetuo.model

import com.criteo.perpetuo.util.json.JsObjectScanner
import spray.json.JsonParser.ParsingException
import spray.json._


object DeploymentRequestParser {
  def parse(jsonInput: String, userName: String): ProtoDeploymentRequest =
    jsonInput.parseJson match {
      case body: JsObject =>
        val scanner = JsObjectScanner(body, Seq("(deployment request)"))
        val productName = scanner.getString("productName")
        if (productName.contains("'")) // fixme: as long as we have Rundeck API 16, but maybe we should configure a validator in plugins
          throw new ParsingException("Single quotes are not supported in product names")
        val version = Version(scanner.get("version"))
        val plan = scanner.getArray("plan").map(parsePlanStep)
        if (plan.isEmpty)
          throw new ParsingException("Plan shall be non-empty")
        val protoDeploymentRequest = ProtoDeploymentRequest(
          productName,
          version,
          plan,
          scanner.getString("comment", Some("")),
          userName
        )

        protoDeploymentRequest

      case unknown => throw new ParsingException(s"Expected a JSON object as request body, got: $unknown")
    }

  def parseTargetExpression(target: JsValue): TargetExpr = {
    target match {
      case JsString(string) if string.nonEmpty => TargetWord(string)
      case JsArray(arr) if arr.nonEmpty => TargetUnion(
        arr.map {
          case JsString(string) if string.nonEmpty => TargetWord(string)
          case unknown => throw new ParsingException(s"Expected a non-empty JSON string in the `target` array, got: $unknown")
        }.toSet
      )
      case unknown => throw new ParsingException(s"Expected `target` to be a non-empty JSON array or string, got: $unknown")
    }
  }

  private def parsePlanStep(step: JsObjectScanner): ProtoDeploymentPlanStep = {
    val name = step.getString("name", Some(""))
    val targetExpr = step.get("target")
    val comment = step.getString("comment", Some(""))
    parseTargetExpression(targetExpr) // validate the target
    ProtoDeploymentPlanStep(name, targetExpr, comment)
  }
}
