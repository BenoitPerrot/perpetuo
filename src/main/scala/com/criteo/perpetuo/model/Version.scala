package com.criteo.perpetuo.model

import slick.lifted.MappedTo
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.util.Try
import scala.util.matching.Regex
import scala.util.matching.Regex.Match


case class PartialVersion(value: JsValue, ratio: Float = 1f)


class Version(serialized: String) extends MappedTo[String] {
  var needsMigration = false // fixme: REMOVE

  // fixme (Try..getOrElse): for transition only, while there are non-structured versions in DB
  val structured: Iterable[PartialVersion] = Try(serialized.parseJson)
    .map {
      case JsArray(arr) =>
        needsMigration |= arr.length == 1
        arr.map(Version.parseVersion)
      case _: JsNumber =>
        needsMigration = true
        Seq(PartialVersion(JsString(serialized))) // fixme: transition only
      case str: JsString =>
        Seq(PartialVersion(str))
      case other =>
        needsMigration = true
        Seq(PartialVersion(other))
    }
    .getOrElse{
      needsMigration = true
      Seq(PartialVersion(JsString(serialized)))
    }
    .map(sv => PartialVersion(Version.replaceAllInStringLeaves(this, sv.value, Version.dropLeading0Regex, _.group(1)), sv.ratio))

  override def toString: String = Version.compactPrint(structured)

  override def value: String = toString

  override def equals(o: scala.Any): Boolean = o.isInstanceOf[Version] && o.asInstanceOf[Version].value == value
}


object Version {
  private val valueField = "value"
  private val ratioField = "ratio"
  private val dropLeading0Regex = """0*(\d+)""".r

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

  private def replaceAllInStringLeaves(root: Version, value: JsValue, regex: Regex, replacer: Match => String): JsValue =
    value match {
      case JsString(s) =>
        val transformed = regex.replaceAllIn(s, replacer)
        root.needsMigration |= transformed != s
        JsString(transformed)
      case JsArray(arr) => JsArray(arr.map(replaceAllInStringLeaves(root, _, regex, replacer)))
      case JsObject(map) => JsObject(map.mapValues(replaceAllInStringLeaves(root, _, regex, replacer)))
      case other => other
    }

  val maxSize: Int = 1024
  val ratioPrecision = 1e-6

  def apply(input: String): Version = new Version(input)

  def compactPrint(versions: Iterable[PartialVersion]): String = {
    {
      if (versions.size == 1)
        versions.head.value match {
          case JsObject(obj) => obj("main")
          case x => x
        }
      else
        versions.map { v => JsObject(valueField -> v.value, ratioField -> JsNumber(v.ratio - (v.ratio % ratioPrecision))) }.toJson
    }.compactPrint
  }
}
