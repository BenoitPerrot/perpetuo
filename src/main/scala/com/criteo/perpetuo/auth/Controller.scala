package com.criteo.perpetuo.auth

import com.criteo.perpetuo.auth.UserFilter._
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.{Controller => BaseController}
import com.twitter.finatra.request.RouteParam
import com.twitter.util.Future
import javax.inject.Inject
import spray.json.{JsObject, JsString}

case class TokenRequest(token: String)
case class LocalUserIdentificationRequest(@RouteParam name: String, request: Request)

class Controller @Inject()(identityProvider: IdentityProvider, permissions: Permissions, jwtEncoder: JWTEncoder) extends BaseController {

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

  get("/api/auth/local-users/:name/jwt") { r: LocalUserIdentificationRequest =>
    r.request.user
      .map(requestingUser =>
        if (permissions.isAuthorized(requestingUser, GeneralAction.administrate)) {
          identityProvider.identifyByName(r.name)
            .map(user =>
              response.ok.plain(user.toJWT(jwtEncoder, expiring = false))
            )
            .rescue {
              case e => Future.value(response.forbidden(e.getMessage))
            }
        } else
          Future.value(response.forbidden)
      )
      .getOrElse(Future.value(response.unauthorized))
  }

  val jsonAuthorizeUrl: String = JsObject("url" -> JsString(identityProvider.authorizeUrl.toString)).compactPrint

  get("/api/auth/authorize-url") { _: Request =>
    response.ok.json(jsonAuthorizeUrl)
  }
}
