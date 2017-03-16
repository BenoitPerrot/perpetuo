package com.criteo.perpetuo.app

import java.net.URL

import com.criteo.perpetuo.auth.{IdentityProvider, JWTEncoder}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule

class AuthModule(config: AppConfig) extends TwitterModule {

  @Singleton
  @Provides
  def providesIdentityProvider: IdentityProvider = {
    IdentityProvider(new URL(config.get[String]("authorize.url")))
  }

  val jwtEncoder = new JWTEncoder(config.get[String]("jwt.secret").getBytes())

  @Singleton
  @Provides
  def providesJWTEncoder: JWTEncoder = {
    jwtEncoder
  }
}
