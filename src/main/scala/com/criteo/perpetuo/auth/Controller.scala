package com.criteo.perpetuo.auth

import javax.inject.Inject

import com.criteo.perpetuo.auth.UserFilter._
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.util.Future
import spray.json.{JsObject, JsString}

case class TokenRequest(token: String)

class Controller @Inject()(identityProvider: IdentityProvider, jwtEncoder: JWTEncoder) extends BaseController {

  get("/api/auth/identity") { r: Request =>
    r.user.map(user => response.ok.plain(user)).getOrElse(response.unauthorized)
  }

  post("/api/auth/identify") { request: TokenRequest =>
    identityProvider.identify(request.token).map { user: User =>
      response.ok.plain(user.toJWT(jwtEncoder))
    }.rescue {
      case e => Future.value(response.unauthorized(e.getMessage))
    }
  }

  val jsonAuthorizeUrl: String = JsObject("url" -> JsString(identityProvider.authorizeUrl.toString)).compactPrint

  get("/api/auth/authorize-url") { _: Request =>
    response.ok.json(jsonAuthorizeUrl)
  }
}
