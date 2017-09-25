package com.criteo.perpetuo.app

import com.criteo.perpetuo.auth._
import com.criteo.perpetuo.config.AppConfig
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule

class AuthModule(config: AppConfig) extends TwitterModule {

  val jwtEncoder = new JWTEncoder(config.get[String]("jwt.secret").getBytes())

  @Singleton
  @Provides
  def providesJWTEncoder: JWTEncoder = {
    jwtEncoder
  }
}
