package com.criteo.perpetuo.auth

import spray.json._

case class User(name: String) {
  def toJWT(encoder: JWTEncoder): String = encoder.encode(JsObject("name" -> JsString(name)).compactPrint)
}

object User {

  def fromJWT(encoder: JWTEncoder, jwt: String): Option[User] = encoder.decode(jwt).map { json =>
    User(json.parseJson.asJsObject.fields("name").asInstanceOf[JsString].value)
  }

  val anonymous = User("anonymous")
}
