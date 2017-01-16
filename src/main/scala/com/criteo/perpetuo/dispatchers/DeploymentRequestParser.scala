package com.criteo.perpetuo.dispatchers

import java.sql.Timestamp

import com.criteo.perpetuo.dao.DeploymentRequest
import com.twitter.finatra.http.exceptions.BadRequestException
import spray.json.{JsArray, JsObject, JsString, JsValue, _}


object DeploymentRequestParser {
  def parse(jsonInput: String): DeploymentRequest =
    jsonInput.parseJson match {
      case body: JsObject =>
        val fields = body.fields
        def read(key: String) = fields.getOrElse(key, missing(key))
        def readStr(key: String, default: Option[String] = None): String = fields.get(key) match {
          case Some(string: JsString) => string.value
          case None => default.getOrElse(missing(key))
          case unknown => throw BadRequestException(s"Expected a string as $key, got: $unknown")
        }

        val targetExpr = read("target")
        val parsedTarget = parseTargetExpression(targetExpr)
        val record = DeploymentRequest(None,
          readStr("productName"), readStr("version"), targetExpr.compactPrint, readStr("reason", Some("")),
          "anonymous", new Timestamp(System.currentTimeMillis))

        record

      case unknown => throw BadRequestException(s"Expected a JSON object as request body, got: $unknown")
    }

  private val defaultTactics: Tactics = Seq(JsObject())
  private val tacticsKey = "tactics"
  private val selectKey = "select"

  private def missing(key: String) = throw BadRequestException(s"Expected to find `$key` at request root")

  private def parseTargetExpression(target: JsValue): Target =
    (target match {
      case string: JsString => Seq((defaultTactics, Seq(string)))
      case arr: JsArray if arr.elements.nonEmpty => arr.elements.map {
        case string: JsString => (defaultTactics, Seq(string))
        case obj: JsObject => parseTargetTerm(obj)
        case unknown => throw BadRequestException(s"Expected a JSON object or string in the `target` array, got: $unknown")
      }
      case obj: JsObject => Seq(parseTargetTerm(obj))
      case unknown => throw BadRequestException(s"Expected `target` to be a non-empty JSON array or object, got: $unknown")
    }).map {
      case (tactics, select) => (
        tactics,
        select.map(_.value match {
          case w if w.nonEmpty => w
          case _ => throw BadRequestException(s"`$selectKey` doesn't accept empty JSON strings as values")
        })
      )
    }

  private def parseTargetTerm(target: JsObject): (Tactics, Seq[JsString]) =
    (
      target.fields.get(tacticsKey).map {
        case arr: JsArray if arr.elements.nonEmpty => arr.elements.map {
          case obj: JsObject => obj
          case unknown => throw BadRequestException(s"Expected a JSON object in the `$tacticsKey` array, got: $unknown")
        }
        case obj: JsObject => Seq(obj)
        case unknown => throw BadRequestException(s"Expected `$tacticsKey` to be a non-empty JSON object or array, got: $unknown")
      }.getOrElse(defaultTactics),

      target.fields.get(selectKey).map {
        case string: JsString => Seq(string)
        case arr: JsArray if arr.elements.nonEmpty =>
          arr.elements.map {
            case string: JsString => string
            case unknown => throw BadRequestException(s"Expected a JSON string in the `$selectKey` array, got: $unknown")
          }
        case unknown => throw BadRequestException(s"Expected `$selectKey` to be a non-empty JSON string or array, got: $unknown")
      }.getOrElse(throw BadRequestException(s"`target` must contain a field `$selectKey`"))
    )
}
