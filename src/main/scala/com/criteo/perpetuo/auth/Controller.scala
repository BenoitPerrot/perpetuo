package com.criteo.perpetuo.auth

import javax.inject.Inject

import com.criteo.perpetuo.auth.UserFilter._
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.{Controller => BaseController}
import spray.json.{JsObject, JsString}

class Controller @Inject() (identityProvider: IdentityProvider) extends BaseController {

  get("/api/auth/identity") { r: Request =>
    r.user.map(user => response.ok.plain(user)).getOrElse(response.forbidden)
  }

  val jsonAuthorizeUrl: String = JsObject("url" -> JsString(identityProvider.authorizeUrl.toString)).compactPrint

  get("/api/auth/authorize-url") { _: Request =>
    response.ok.json(jsonAuthorizeUrl)
  }
}
