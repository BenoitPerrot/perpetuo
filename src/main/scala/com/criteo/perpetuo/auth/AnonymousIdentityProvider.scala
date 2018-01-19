package com.criteo.perpetuo.auth

import com.twitter.util.Future

object AnonymousIdentityProvider extends IdentityProvider {
  val anonymous: User = User("anonymous")

  override def identify(token: String): Future[User] = Future.value(anonymous)

  override def authorizeUrl: String = "/identify#access_token="

  override def identifyByName(userName: String): Future[User] =
    if (userName == anonymous.name)
      Future.value(anonymous)
    else
      Future.exception(new Exception(s"Only ${anonymous.name} is known"))
}
