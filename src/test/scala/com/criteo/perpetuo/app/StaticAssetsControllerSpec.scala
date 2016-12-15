package com.criteo.perpetuo.app

import com.twitter.finagle.http.Status.Ok
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.HttpServer
import com.twitter.finatra.http.filters.{CommonFilters, LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.test.EmbeddedHttpServer
import com.twitter.inject.server.FeatureTest
import com.typesafe.config.{Config, ConfigFactory}


/**
  * An integration test for [[StaticAssetsController]].
  */
class StaticAssetsControllerSpec extends FeatureTest {

  val server = new EmbeddedHttpServer(new HttpServer {

    val config: Config = ConfigFactory.load()

    override def configureHttp(router: HttpRouter) {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .add[StaticAssetsController]
    }
  })

  "The StaticAssetsController" should {

    "say hello on /" in {
      server.httpGet(
        path = "/",
        andExpect = Ok,
        withBody = "hello"
      )
    }

  }

}
