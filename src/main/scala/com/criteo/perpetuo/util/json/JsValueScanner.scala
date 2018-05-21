package com.criteo.perpetuo.util.json

import spray.json.JsonParser.ParsingException

trait JsValueScanner {
  val path: Seq[String]

  protected def reportMissing(key: String) =
    throw new ParsingException(s"${path.mkString(".")}: while required, no field named `$key` could be found")

  protected def reportWrongType(key: String, expecting: String, encountered: String) =
    throw new ParsingException(s"${(path :+ key).mkString(".")}: while expecting $expecting, encountered $encountered")
}
