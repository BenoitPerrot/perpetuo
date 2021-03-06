package com.criteo.perpetuo.app

import com.twitter.finagle.http.Status.Ok
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.filters.{LoggingMDCFilter, TraceIdMDCFilter}
import com.twitter.finatra.http.routing.HttpRouter
import com.twitter.finatra.http.{EmbeddedHttpServer, HttpServer}
import com.twitter.inject.Test


/**
  * An integration test for [[StaticAssetsController]].
  */
class StaticAssetsControllerSpec extends Test {

  val server = new EmbeddedHttpServer(new HttpServer {

    override def configureHttp(router: HttpRouter): Unit = {
      router
        .filter[LoggingMDCFilter[Request, Response]]
        .filter[TraceIdMDCFilter[Request, Response]]
        .filter[CommonFilters]
        .add(new StaticAssetsController(Seq()))
    }
  })

  test("The StaticAssetsController answers on /") {
    server.httpGet(
      path = "/",
      andExpect = Ok
    )
  }

}
