package com.criteo.perpetuo.auth

import java.net.URL

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http._
import com.twitter.util.Future
import spray.json._


class OpenAmIdentityProvider(authorize: URL, tokeninfo: URL) extends IdentityProvider {

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

}
