package com.criteo.perpetuo.auth

import javax.inject.Singleton

import com.google.inject.Inject
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Future

object UserFilter {
  val UserField = Request.Schema.newField[Option[User]]()

  implicit class SyntacticSugar(val request: Request) {
    def user: Option[User] = request.ctx(UserField)
  }

  val cookieName = "jwt"
}

@Singleton
class UserFilter @Inject() (jwtEncoder: JWTEncoder) extends SimpleFilter[Request, Response] {

  private def setUser(request: Request) = {
    request.ctx.update(UserFilter.UserField,
      request.cookies.get(UserFilter.cookieName)
        .map(jwt => User.fromJWT(jwtEncoder, jwt.value))
        .getOrElse(Some(User.anonymous)))
  }

  override def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    setUser(request)
    service(request)
  }
}
