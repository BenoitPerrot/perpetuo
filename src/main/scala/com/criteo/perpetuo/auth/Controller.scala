package com.criteo.perpetuo.auth

import com.criteo.perpetuo.auth.UserFilter._
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.{Controller => BaseController}

class Controller extends BaseController {

  get("/api/auth/identity") { r: Request =>
    r.user.map(user => response.ok.plain(user)).getOrElse(response.forbidden)
  }
}
