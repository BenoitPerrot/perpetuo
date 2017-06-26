package com.criteo.perpetuo.model

import slick.lifted.MappedTo
import spray.json.DefaultJsonProtocol._
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.util.Try


case class SingleVersion(value: Map[String, String], ratio: Float = 1f)


class Version(serialized: String) extends MappedTo[String] {
  // fixme (Try..getOrElse): for transition only, while there are non-structured versions in DB
  val structured: Iterable[SingleVersion] = Try(serialized.parseJson.asInstanceOf[JsArray].elements)
    .map(_.map(Version.parseVersion))
    .getOrElse(Seq(SingleVersion(Map("main" -> serialized))))
    .map(sv => SingleVersion(sv.value.mapValues(v => Version.dropLeading0Regex.replaceAllIn(v, _.group(1))), sv.ratio))

  // compute the standardized representation, usable by Slick's `sortBy` thanks to `MappedTo`
  val value: String = try {
    val uniformed = Version.compactPrint(
      structured.map(sv => {
        val value = sv.value.mapValues(v => Version.numberRegex.replaceAllIn(v, { m =>
          val nb = m.matched
          val prefix = Version.numberBaseField.length - nb.length
          assert(prefix >= 0)
          Version.numberBaseField.slice(0, prefix) + nb
        }))
        SingleVersion(value, sv.ratio)
      })
    )
    assert(uniformed.length <= Version.maxSize)
    uniformed
  }
  catch {
    case _: AssertionError => Version.compactPrint(structured)
  }

  // todo: break the API to use structured versions everywhere (update the plugins and the front-end)
  override def toString: String = structured.head.value.head._2

  override def equals(o: scala.Any): Boolean = o.isInstanceOf[Version] && o.asInstanceOf[Version].value == value
}


object Version {
  private val valueField = "value"
  private val ratioField = "ratio"
  private val numberBaseField = "0" * System.currentTimeMillis.toString.length
  private val numberRegex = """\d+""".r
  private val dropLeading0Regex = """0*(\d+)""".r

  private val parseVersion: JsValue => SingleVersion = {
    case JsObject(obj) =>
      val value = obj.getOrElse(valueField, throw new ParsingException(s"Expected to find a `$valueField` in every `version`")) match {
        case JsObject(o) => o.map {
          case (sKey, JsString(string)) => (sKey, string)
          case (uKey, unsupported) => throw new ParsingException(s"Expected a JSON string at `version.$valueField.$uKey`, got: $unsupported")
        }
        case unsupported => throw new ParsingException(s"Expected a JSON object as `$valueField` in every `version`, got: $unsupported")
      }
      val ratio = obj.getOrElse(ratioField, JsNumber(1)) match {
        case JsNumber(r) if r >= 0 && r <= 1 => r.floatValue
        case unexpected => throw new ParsingException(s"Expected a number in [0; 1] as `$ratioField` in every `version`, got: $unexpected")
      }
      SingleVersion(value, ratio)
    case unknown => throw new ParsingException(s"Expected JSON objects in `version`, got: $unknown")
  }

  val maxSize: Int = 1024 // todo: increase that to deal with partial deployments!

  def apply(input: String): Version = new Version(input)

  def compactPrint(versions: Iterable[SingleVersion]): String = {
    versions.map { v =>
      val map = Map(valueField -> v.value.toJson)
      if (v.ratio != 1f) map ++ Map(ratioField -> JsNumber(v.ratio)) else map
    }.toJson.compactPrint
  }
}
