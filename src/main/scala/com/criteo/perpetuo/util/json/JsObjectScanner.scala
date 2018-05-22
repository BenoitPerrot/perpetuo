package com.criteo.perpetuo.util.json

import spray.json.JsonParser.ParsingException
import spray.json.{JsObject, JsString, JsValue}

case class JsObjectScanner(o: JsObject, path: Seq[String]) {
  def get(key: String): JsValue =
    o.fields.getOrElse(key, reportMissing(key))

  def getString(key: String, default: Option[String] = None): String =
    o.fields.get(key) match {
      case Some(JsString(string)) => string
      case None => default.getOrElse(reportMissing(key))
      case Some(unknown) => reportWrongType(key, "a string", s"$unknown (${unknown.getClass.getSimpleName})")
    }

  private def reportMissing(key: String) =
    throw new ParsingException(s"${path.mkString(".")}: while required, no field named `$key` could be found")

  private def reportWrongType(key: String, expecting: String, encountered: String) =
    throw new ParsingException(s"${path.mkString(".")}: while expecting $expecting, encountered $encountered")
}
