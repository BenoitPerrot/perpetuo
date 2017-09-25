package com.criteo.perpetuo.auth

import com.twitter.util.Future

class AnonymousIdentityProvider extends IdentityProvider {
  override def identify(token: String): Future[User] = Future.value(User("anonymous"))

  override def authorizeUrl: String = "/identify#access_token="
}
