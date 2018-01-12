package com.criteo.perpetuo.auth

import spray.json._

case class User(name: String, groupNames: Set[String] = Set("Users")) { // TODO: actually create this set
  def toJWT(encoder: JWTEncoder): String = encoder.encode(JsObject("name" -> JsString(name)).compactPrint)
}

object User {
  val maxSize: Int = 64

  def fromJWT(encoder: JWTEncoder, jwt: String): Option[User] = encoder.decode(jwt).map { json =>
    User(json.parseJson.asJsObject.fields("name").asInstanceOf[JsString].value.take(maxSize))
  }
}
