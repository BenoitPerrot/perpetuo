package com.criteo.perpetuo.auth

import com.twitter.util.Future

trait IdentityProvider {
  def identify(token: String): Future[User]
  def authorizeUrl: String
}
