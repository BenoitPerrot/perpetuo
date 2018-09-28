package com.criteo.perpetuo.auth

import com.twitter.util.Future

object AnonymousIdentityProvider extends IdentityProvider {
  val anonymous: User = User("anonymous")

  override def identify(token: String): Future[User] = Future.value(anonymous)

  override def authorizeUrl: String = "/identify#access_token="
}
