package com.criteo.perpetuo.model

import java.sql.Timestamp

import com.criteo.perpetuo.dispatchers.{Tactics, TargetExpr, TargetTerm}
import spray.json.JsonParser.ParsingException
import spray.json._


object DeploymentRequestParser {
  def parse(jsonInput: String, userName: String): DeploymentRequestAttrs =
    jsonInput.parseJson match {
      case body: JsObject =>
        val fields = body.fields
        def read(key: String): JsValue = fields.getOrElse(key, missing(key))
        def readStr(key: String, default: Option[String] = None): String = fields.get(key) match {
          case Some(JsString(string)) => string
          case None => default.getOrElse(missing(key))
          case Some(unknown) => throw new ParsingException(s"Expected a string as $key, got: $unknown (${unknown.getClass.getSimpleName})")
        }

        val versionArray = read("version") match {
          case JsString(string) => Version.compactPrint(Seq(PartialVersion(Map("main" -> string)))) // fixme: transition only
          case jsArr: JsArray if jsArr.elements.nonEmpty => jsArr.compactPrint
          case unknown => throw new ParsingException(s"Expected `version` to be a non-empty JSON array, got: $unknown")
        }
        if (versionArray.length > Version.maxSize)
          throw new ParsingException(s"Version is too long")
        val version = Version(versionArray)
        if ((version.structured.map(_.ratio).sum - 1f).abs > 1e-6f)
          throw new ParsingException("Sum of ratios must equal 1")
        val targetExpr = read("target")
        val attrs = new DeploymentRequestAttrs(
          readStr("productName"),
          version,
          targetExpr.compactPrint,
          readStr("comment", Some("")),
          userName,
          new Timestamp(System.currentTimeMillis)
        )
        attrs.parsedTarget // validate the target

        attrs

      case unknown => throw new ParsingException(s"Expected a JSON object as request body, got: $unknown")
    }

  private val tacticsKey = "tactics"
  private val selectKey = "select"

  private def missing(key: String) = throw new ParsingException(s"Expected to find `$key` at request root")

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
      }.map(Some(_)).getOrElse(None),

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
}
