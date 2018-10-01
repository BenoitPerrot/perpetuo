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

  // todo? standardize expressions (in DB history)
  def parseRootTargetExpression(target: JsValue): TargetExpr = {
    target match {
      case JsArray(arr) if arr.nonEmpty =>
        TargetUnion(arr.map(parseTargetExpression).toSet)
      case _ => parseTargetExpression(target)
    }
  }

  def parseTargetExpression(target: JsValue): TargetExpr = {
    target match {
      case JsString(string) if string.nonEmpty => TargetWord(string)
      case JsObject(fields) =>
        if (fields.isEmpty)
          TargetTop
        else if (fields.size == 1)
          fields.head match {
            case ("union", JsArray(arr)) =>
              TargetUnion(arr.map(parseTargetExpression).toSet)
            case ("intersection", JsArray(arr)) =>
              TargetIntersection(arr.map(parseTargetExpression).toSet)
            case _ =>
              throw new ParsingException(s"Unexpected target element: $target")
          }
        else
          throw new ParsingException(s"In target expressions, objects must contain at most one key; got the object $target")
      case _ => throw new ParsingException(s"Unexpected target element: $target")
    }
  }

  private def parsePlanStep(step: JsObjectScanner): ProtoDeploymentPlanStep = {
    val name = step.getString("name", Some(""))
    val targetExpr = step.get("target")
    val comment = step.getString("comment", Some(""))
    parseRootTargetExpression(targetExpr) // validate the target
    ProtoDeploymentPlanStep(name, targetExpr, comment)
  }
}
