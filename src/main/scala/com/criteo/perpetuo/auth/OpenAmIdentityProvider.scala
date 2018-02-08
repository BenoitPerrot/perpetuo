package com.criteo.perpetuo.auth

import java.net.URL

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http._
import com.twitter.finagle.{Http, Service}
import com.twitter.util.Future
import com.typesafe.config.Config
import spray.json._


class OpenAmIdentityProvider(authorize: URL, tokeninfo: URL, localUserNames: Set[String]) extends IdentityProvider {

  def authorizeUrl: String = authorize.toString

  lazy val client: Service[Request, Response] = ClientBuilder()
    .stack(Http.Client())
    .hosts(s"${tokeninfo.getHost}:${
      tokeninfo.getPort match {
        case -1 => tokeninfo.getDefaultPort
        case value => value
      }
    }")
    .tlsWithoutValidation()
    .hostConnectionLimit(1)
    .failFast(false)
    .build()

  def request(token: String): Future[Response] = {
    client(
      RequestBuilder()
        .url(s"$tokeninfo&access_token=$token")
        .buildGet()
    )
  }

  override def identify(token: String): Future[User] = {
    request(token).flatMap(r => {
      r.status match {
        case Status.Ok => Future.value(new User(r.contentString.parseJson.asJsObject.fields("uid").asInstanceOf[JsString].value))
        case _ => Future.exception(new Exception("Authentication failed"))
      }
    })
  }

  override def identifyByName(userName: String): Future[User] =
    if (localUserNames.contains(userName))
      Future.value(User(userName))
    else
      Future.exception(new Exception(s"$userName is not in the set of users that can be identified by name only"))

}

object OpenAmIdentityProvider {
  def fromConfig(config: Config): OpenAmIdentityProvider = {
    val localUserNames = config.tryGet[Seq[String]]("localUserNames").getOrElse(Seq()).toSet
    new OpenAmIdentityProvider(
      new URL(config.getString("authorize.url")),
      new URL(config.getString("tokeninfo.url")),
      localUserNames)
  }
}