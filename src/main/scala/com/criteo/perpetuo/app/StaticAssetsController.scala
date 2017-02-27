package com.criteo.perpetuo.app

import com.twitter.finagle.http.Request
import com.twitter.finatra.http.Controller

/**
  * Serve static assets
  */
class StaticAssetsController() extends Controller {

  Array(
    "/manifest.json",
    "/src/:*",
    "/bower_components/:*"
  ).foreach(uri => {
    get(uri) { request: Request =>
      response.ok.file(request.uri)
    }
  })

  get("/:*") { request: Request =>
    response.ok.file("index.html")
  }

}
