package com.criteo.perpetuo.auth

import com.criteo.perpetuo.auth.UserFilter._
import com.twitter.finagle.http.{Request, Status}
import com.twitter.finatra.http.exceptions.{ForbiddenException, HttpException}
import com.twitter.util.Future

trait Authenticator {
  def authenticate[T](r: Request)(callback: PartialFunction[User, Future[Option[T]]]): Future[Option[T]] = {
    r.user
      .map(callback.orElse { case _ => throw ForbiddenException() })
      .getOrElse(throw HttpException(Status.Unauthorized))
  }
}
