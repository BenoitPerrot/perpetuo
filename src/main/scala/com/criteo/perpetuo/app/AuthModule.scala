package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth._
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.typesafe.config.Config

class AuthModule(config: Config) extends TwitterModule {

  val jwtEncoder = new JWTEncoder(config.getString("jwt.secret").getBytes())

  @Singleton
  @Provides
  def providesJWTEncoder: JWTEncoder = {
    jwtEncoder
  }
}
