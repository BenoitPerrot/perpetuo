package com.criteo.perpetuo.app

import com.google.inject.Provides
import com.jakehschwartz.finatra.swagger.{SwaggerModule => BaseSwaggerModule}
import io.swagger.models.{Info, Swagger}

class SwaggerModule extends BaseSwaggerModule {

  val swagger = new Swagger

  @Provides
  def providesSwagger: Swagger = {
    val info = new Info()
        .description("REST API for Perpetuo")
        .version("1.0")
        .title("Perpetuo API")
    swagger.info(info)
    swagger
  }

}
