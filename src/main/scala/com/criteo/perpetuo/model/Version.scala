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
  // fixme (Try..getOrElse): for transition only, while there are non-structured versions in DB
  val structured: Iterable[PartialVersion] = Try(serialized.parseJson)
    .map {
      case JsArray(arr) => arr.map(Version.parseVersion)
      case _: JsNumber => Seq(PartialVersion(JsString(serialized))) // fixme: transition only
      case other => Seq(PartialVersion(other))
    }
    .getOrElse(Seq(PartialVersion(JsString(serialized))))
    .map(sv => PartialVersion(Version.replaceAllInStringLeaves(sv.value, Version.dropLeading0Regex, _.group(1)), sv.ratio))

  // compute the standardized representation, usable by Slick's `sortBy` thanks to `MappedTo`
  val value: String = try {
    val uniformed = Version.compactPrint(
      structured.map(sv => {
        val value = Version.replaceAllInStringLeaves(sv.value, Version.numberRegex, { m =>
          val nb = m.matched
          val prefix = Version.numberBaseField.length - nb.length
          assert(prefix >= 0)
          Version.numberBaseField.slice(0, prefix) + nb
        })
        PartialVersion(value, sv.ratio)
      })
    )
    assert(uniformed.length <= Version.maxSize)
    uniformed
  }
  catch {
    case _: AssertionError => toString
  }

  override def toString: String = Version.compactPrint(structured)

  override def equals(o: scala.Any): Boolean = o.isInstanceOf[Version] && o.asInstanceOf[Version].value == value
}


object Version {
  private val valueField = "value"
  private val ratioField = "ratio"
  private val numberBaseField = "0" * System.currentTimeMillis.toString.length
  private val numberRegex = """\d+""".r
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

  private def replaceAllInStringLeaves(value: JsValue, regex: Regex, replacer: Match => String): JsValue =
    value match {
      case JsString(s) => JsString(regex.replaceAllIn(s, replacer))
      case JsArray(arr) => JsArray(arr.map(replaceAllInStringLeaves(_, regex, replacer)))
      case JsObject(map) => JsObject(map.mapValues(replaceAllInStringLeaves(_, regex, replacer)))
      case other => other
    }

  val maxSize: Int = 1024
  val ratioPrecision = 1e-6

  def apply(input: String): Version = new Version(input)

  def compactPrint(versions: Iterable[PartialVersion]): String = {
    {
      if (versions.size == 1)
        versions.head.value
      else
        versions.map { v => JsObject(valueField -> v.value, ratioField -> JsNumber(v.ratio - (v.ratio % ratioPrecision))) }.toJson
    }.compactPrint
  }
}
