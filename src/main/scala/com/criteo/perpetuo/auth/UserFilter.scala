package com.criteo.perpetuo.auth

import com.google.inject.Inject
import com.twitter.finagle.http.Request.Schema
import com.twitter.finagle.http.{Request, Response, Status}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Future
import javax.inject.Singleton

object UserFilter {
  val UserField: Schema.Field[Option[User]] = Schema.newField[Option[User]]()

  implicit class SyntacticSugar(val request: Request) {
    def user: Option[User] = request.ctx(UserField)
  }

  def decorateRequest(r: Request, u: Option[User]): Schema.Record =
    r.ctx.update(UserFilter.UserField, u)

  val cookieName = "jwt"
}

@Singleton
class UserFilter @Inject()(jwtEncoder: JWTEncoder, identityProvider: IdentityProvider) extends SimpleFilter[Request, Response] {

  private val loginHeader = "X-Perpetuo-Login"

  private def setUser(request: Request) = {
    UserFilter.decorateRequest(request,
      request.headerMap.get("Authorization").orElse(request.cookies.get(UserFilter.cookieName).map(_.value))
        .flatMap(jwt => User.fromJWT(jwtEncoder, jwt))
    )
  }

  override def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    setUser(request)
    service(request)
      .map { resp =>
        if (resp.status == Status.Unauthorized) {
          resp.headerMap.set(loginHeader, identityProvider.authorizeUrl)
          resp.headerMap.set("Access-Control-Expose-Headers", loginHeader)
        }
        resp
      }
  }
}
