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
        def read(key: String) = fields.getOrElse(key, missing(key))
        def readStr(key: String, default: Option[String] = None): String = fields.get(key) match {
          case Some(string: JsString) => string.value
          case None => default.getOrElse(missing(key))
          case unknown => throw new ParsingException(s"Expected a string as $key, got: $unknown")
        }

        val versionArray = read("version") match {
          case JsString(string) => Version.compactPrint(Seq(SingleVersion(Map("main" -> string)))) // fixme: transition only
          case jsArr: JsArray if jsArr.elements.nonEmpty => jsArr.compactPrint
          case unknown => throw new ParsingException(s"Expected `version` to be a non-empty JSON array, got: $unknown")
        }
        if (versionArray.length > Version.maxSize)
          throw new ParsingException(s"Version is too long")
        val version = Version(versionArray)
        if (version.structured.map(_.ratio).sum != 1f)
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
      case string: JsString => Set((None, Set(string)))
      case arr: JsArray if arr.elements.nonEmpty => arr.elements.map {
        case string: JsString => (None, Set(string))
        case obj: JsObject => parseTargetTerm(obj)
        case unknown => throw new ParsingException(s"Expected a JSON object or string in the `target` array, got: $unknown")
      }.toSet
      case obj: JsObject => Set(parseTargetTerm(obj))
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

  private def parseTargetTerm(target: JsObject): (Option[Tactics], Set[JsString]) =
    (
      target.fields.get(tacticsKey).map {
        case arr: JsArray if arr.elements.nonEmpty => arr.elements.map {
          case obj: JsObject => obj
          case unknown => throw new ParsingException(s"Expected a JSON object in the `$tacticsKey` array, got: $unknown")
        }.toSet
        case obj: JsObject => Set(obj)
        case unknown => throw new ParsingException(s"Expected `$tacticsKey` to be a non-empty JSON object or array, got: $unknown")
      }.map(Some(_)).getOrElse(None),

      target.fields.get(selectKey).map {
        case string: JsString => Set(string)
        case arr: JsArray if arr.elements.nonEmpty =>
          arr.elements.map {
            case string: JsString => string
            case unknown => throw new ParsingException(s"Expected a JSON string in the `$selectKey` array, got: $unknown")
          }.toSet
        case unknown => throw new ParsingException(s"Expected `$selectKey` to be a non-empty JSON string or array, got: $unknown")
      }.getOrElse(throw new ParsingException(s"`target` must contain a field `$selectKey`"))
    )
}
