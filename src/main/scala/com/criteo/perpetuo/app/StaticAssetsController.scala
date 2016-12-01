package com.criteo.perpetuo.app

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

/**
  * Serve static assets
  */
class StaticAssetsController() extends Controller {

  get("/") { request: Request =>
    response.ok.plain("hello")
  }

}
