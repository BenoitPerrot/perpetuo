package com.criteo.perpetuo.auth

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._

case class User(name: String, groupNames: Set[String] = Set()) {
  def toJWT(encoder: JWTEncoder, expiring: Boolean = true): String = {
    encoder.encode(JsObject(
      Map("name" -> JsString(name))
        ++ (if (groupNames.nonEmpty) Map("groupNames" -> groupNames.toVector.toJson) else Map.empty)
        ++ (if (expiring) Map("iat" -> JsNumber(User.currentTimestamp)) else Map.empty)
      // iat = "issued at": https://tools.ietf.org/html/rfc7519#section-4.1.6
    ).compactPrint)
  }
}

object User {
  val maxValidity: FiniteDuration = 1.day // todo: to lower when no click will be needed to renew the token

  def currentTimestamp: Long = System.currentTimeMillis() / 1000

  def fromJWT(encoder: JWTEncoder, jwt: String): Option[User] = encoder.decode(jwt).flatMap { json =>
    val values = json.parseJson.asJsObject.fields
    val expired = values.get("iat").exists(_.asInstanceOf[JsNumber].value + maxValidity.toSeconds < currentTimestamp)
    if (expired)
      None
    else
      Some(User(
        values("name").asInstanceOf[JsString].value,
        values.get("groupNames").map(
          _.asInstanceOf[JsArray].elements
            .map(_.asInstanceOf[JsString].value)
            .toSet
        ).getOrElse(Set())
      ))
  }
}
