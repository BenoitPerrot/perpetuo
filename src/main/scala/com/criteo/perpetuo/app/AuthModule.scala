package com.criteo.perpetuo.app

import java.net.URL

import com.criteo.perpetuo.auth.{IdentityProvider, JWTEncoder, OpenAmIdentityProvider}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule

class AuthModule(config: AppConfig) extends TwitterModule {

  @Singleton
  @Provides
  def providesIdentityProvider: IdentityProvider = {
    new OpenAmIdentityProvider(new URL(config.get("authorize.url")), new URL(config.get("tokeninfo.url")))
  }

  val jwtEncoder = new JWTEncoder(config.get[String]("jwt.secret").getBytes())

  @Singleton
  @Provides
  def providesJWTEncoder: JWTEncoder = {
    jwtEncoder
  }
}
