package com.criteo.perpetuo.model

import slick.lifted.MappedTo
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException
import spray.json._


case class PartialVersion(value: JsValue, ratio: Float = 1f)


class Version(serialized: String) extends MappedTo[String] {
  val structured: Iterable[PartialVersion] = serialized.parseJson match {
      case JsArray(arr) => arr.map(Version.parseVersion)
      case other => Seq(PartialVersion(other))
    }

  override def toString: String = serialized

  override def value: String = serialized

  override def equals(o: scala.Any): Boolean = o.isInstanceOf[Version] && o.asInstanceOf[Version].value == value
}


object Version {
  private val valueField = "value"
  private val ratioField = "ratio"

  private val parseVersion: JsValue => PartialVersion = {
    case JsObject(obj) =>
      val value = obj.getOrElse(valueField, throw new ParsingException(s"Expected to find a `$valueField` in every `version`"))
      val ratio = obj.getOrElse(ratioField, JsNumber(1)) match {
        case JsNumber(r) if r >= 0 && r <= 1 => r.floatValue
        case unexpected => throw new ParsingException(s"Expected a number in [0; 1] as `$ratioField` in every `version`, got: $unexpected")
      }
      PartialVersion(value, ratio)
    case unknown => throw new ParsingException(s"Expected JSON objects in `version`, got: $unknown")
  }

  val maxSize: Int = 1024
  val ratioPrecision = 1e-6

  def apply(input: String): Version = new Version(input)

  def apply(input: JsValue): Version = {
    val versionArray = input match {
      case str: JsString => Version.compactPrint(Seq(PartialVersion(str))) // todo: uniform call from UI?
      case jsArr: JsArray if jsArr.elements.nonEmpty => jsArr.compactPrint
      case unknown => throw new ParsingException(s"Expected `version` to be a non-empty JSON array, got: $unknown")
    }
    if (versionArray.contains("'")) // fixme: as long as we have Rundeck API 16, but maybe we should configure a validator in plugins
      throw new ParsingException("Single quotes are not supported in versions")
    if (versionArray.length > Version.maxSize)
      throw new ParsingException(s"Version is too long")
    val version = Version(versionArray)
    if ((version.structured.map(_.ratio).sum - 1f).abs > Version.ratioPrecision)
      throw new ParsingException("Sum of ratios must equal 1")
    version
  }

  def compactPrint(versions: Iterable[PartialVersion]): String = {
    {
      if (versions.size == 1)
        versions.head.value
      else
        versions.map { v => JsObject(valueField -> v.value, ratioField -> JsNumber(v.ratio - (v.ratio % ratioPrecision))) }.toJson
    }.compactPrint
  }
}
