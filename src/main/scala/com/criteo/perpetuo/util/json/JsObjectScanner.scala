package com.criteo.perpetuo.util.json

import spray.json.{JsArray, JsBoolean, JsObject, JsString, JsValue}

case class JsObjectScanner(o: JsObject, path: Seq[String]) extends JsValueScanner {
  def get(key: String): JsValue =
    o.fields.getOrElse(key, reportMissing(key))

  def getString(key: String, default: Option[String] = None): String =
    o.fields.get(key) match {
      case Some(JsString(string)) => string
      case None => default.getOrElse(reportMissing(key))
      case Some(unknown) => reportWrongType(key, "a string", s"$unknown (${unknown.getClass.getSimpleName})")
    }

  def getArray(key: String): JsArrayScanner =
    get(key) match {
      case a: JsArray => JsArrayScanner(a, path :+ key)
      case unknown => reportWrongType(key, "an array", s"$unknown (${unknown.getClass.getSimpleName})")
    }

  def getBoolean(key: String, default: Option[Boolean] = None): Boolean =
    o.fields.get(key) match {
      case Some(JsBoolean(b)) => b
      case None => default.getOrElse(reportMissing(key))
      case Some(unknown) => reportWrongType(key, "a boolean", s"$unknown (${unknown.getClass.getSimpleName})")
    }
}
