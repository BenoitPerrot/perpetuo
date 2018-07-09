package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine.{Tactics, TargetExpr, TargetTerm}
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
        // TODO: remove once migration complete:
        assert(plan.size == 1)
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

  private val tacticsKey = "tactics"
  private val selectKey = "select"

  def parseTargetExpression(target: JsValue): TargetExpr = {
    val whereTacticsIsOptional: Set[(Option[Tactics], Set[JsString])] = target match {
      case jsString: JsString => Set((None, Set(jsString)))
      case JsArray(arr) if arr.nonEmpty => arr.map {
        case jsString: JsString => (None, Set(jsString))
        case JsObject(obj) => parseTargetTerm(obj)
        case unknown => throw new ParsingException(s"Expected a JSON object or string in the `target` array, got: $unknown")
      }.toSet
      case JsObject(obj) => Set(parseTargetTerm(obj))
      case unknown => throw new ParsingException(s"Expected `target` to be a non-empty JSON array or object, got: $unknown")
    }
    whereTacticsIsOptional.map {
      case (tacticsOption, selectWithJsonValues) =>
        val s = selectWithJsonValues.map(_.value match {
          case w if w.nonEmpty => w
          case _ => throw new ParsingException(s"`$selectKey` doesn't accept empty JSON strings as values")
        })
        tacticsOption.map(TargetTerm(_, s)).getOrElse(TargetTerm(select = s))
    }
  }

  private def parseTargetTerm(target: Map[String, JsValue]): (Option[Tactics], Set[JsString]) =
    (
      target.get(tacticsKey).map {
        case JsArray(arr) if arr.nonEmpty => arr.map {
          case jsObj: JsObject => jsObj
          case unknown => throw new ParsingException(s"Expected a JSON object in the `$tacticsKey` array, got: $unknown")
        }.toSet
        case jsObj: JsObject => Set(jsObj)
        case unknown => throw new ParsingException(s"Expected `$tacticsKey` to be a non-empty JSON object or array, got: $unknown")
      },

      target.get(selectKey).map {
        case jsString: JsString => Set(jsString)
        case JsArray(arr) if arr.nonEmpty =>
          arr.map {
            case string: JsString => string
            case unknown => throw new ParsingException(s"Expected a JSON string in the `$selectKey` array, got: $unknown")
          }.toSet
        case unknown => throw new ParsingException(s"Expected `$selectKey` to be a non-empty JSON string or array, got: $unknown")
      }.getOrElse(throw new ParsingException(s"`target` must contain a field `$selectKey`"))
    )

  private def parsePlanStep(step: JsObjectScanner): ProtoDeploymentPlanStep = {
    val name = step.getString("name", Some(""))
    val targetExpr = step.get("target")
    val comment = step.getString("comment", Some(""))
    parseTargetExpression(targetExpr) // validate the target
    ProtoDeploymentPlanStep(name, targetExpr, comment)
  }
}
