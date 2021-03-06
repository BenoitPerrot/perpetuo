package com.criteo.perpetuo.model

import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException
import spray.json._


case class PartialVersion(value: String, ratio: Float = 1f)


case class Version(serialized: String) {
  lazy val structured: Iterable[PartialVersion] = Version.toStructured(serialized)

  override def toString: String = serialized
}


object Version {
  val ratioPrecision = 1e-6

  private val valueField = "value"
  private val ratioField = "ratio"

  private val partialFromJson: JsValue => PartialVersion = {
    case JsObject(obj) =>
      val value = obj.getOrElse(valueField, throw new ParsingException(s"Expected to find a `$valueField` in every `version`")) match {
        case JsString(v) => v
        case unexpected => throw new ParsingException(s"Expected a string `$valueField` in every `version`, got: $unexpected")
      }
      val ratio = obj.getOrElse(ratioField, JsNumber(1)) match {
        case JsNumber(r) if 0 <= r && r <= 1 => r.floatValue
        case unexpected => throw new ParsingException(s"Expected a number in [0; 1] as `$ratioField` in every `version`, got: $unexpected")
      }
      PartialVersion(value, ratio)
    case unknown => throw new ParsingException(s"Expected JSON objects in `version`, got: $unknown")
  }

  def apply(input: JsValue): Version = {
    val versionArray = input match {
      case JsString(str) => Version.compactPrint(Seq(PartialVersion(str))) // todo: uniform call from UI?
      case jsArr: JsArray if jsArr.elements.nonEmpty => jsArr.compactPrint
      case unknown => throw new ParsingException(s"Expected `version` to be a non-empty JSON array, got: $unknown")
    }
    if (versionArray.contains("'")) // fixme: as long as we have Rundeck API 16, but maybe we should configure a validator in plugins
      throw new ParsingException("Single quotes are not supported in versions")
    val version = Version(versionArray)
    if ((version.structured.map(_.ratio).sum - 1f).abs > Version.ratioPrecision)
      throw new ParsingException("Sum of ratios must equal 1")
    version
  }

  def compactPrint(structured: Iterable[PartialVersion]): String = {
    {
      if (structured.size == 1)
        JsString(structured.head.value)
      else
        structured.map { v => JsObject(valueField -> JsString(v.value), ratioField -> JsNumber(v.ratio - (v.ratio % ratioPrecision))) }.toJson
    }.compactPrint
  }

  def toStructured(input: String): Iterable[PartialVersion] = input.parseJson match {
    case JsArray(arr) => arr.map(partialFromJson)
    case JsString(v) => Seq(PartialVersion(v))
    case _ => throw new ParsingException("Should not be there")
  }
}
