package com.criteo.perpetuo.app

import com.criteo.perpetuo.dao.UnknownProduct
import com.criteo.perpetuo.engine.dispatchers.UnprocessableIntent
import com.criteo.perpetuo.engine.{Conflict, MissingInfo, RejectingError, UnavailableAction}
import com.twitter.finagle.http.Status
import com.twitter.finagle.{TimeoutException => FinagleTimeout}
import com.twitter.finatra.http.exceptions.{BadRequestException, HttpException, HttpResponseException}
import com.twitter.finatra.http.response.ResponseBuilder
import com.twitter.util.{TimeoutException => TwitterTimeout}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}


trait ExceptionsToHttpStatusTranslation {
  protected def response: ResponseBuilder

  def await[T](future: => Future[T], maxDuration: Duration): T =
    handleExceptions(Await.result(future, maxDuration))

  private def toHttpResponseException(e: RejectingError, status: Status): HttpResponseException = {
    val body = Map("errors" -> Seq(e.msg)) ++ e.detail
    new HttpResponseException(response.EnrichedResponse(status).json(body))
  }

  def handleExceptions[T](action: => T): T =
    try {
      action
    }
    catch {
      case e: FinagleTimeout => throw HttpException(Status.GatewayTimeout, e.getMessage)
      case e: TwitterTimeout => throw HttpException(Status.GatewayTimeout, e.getMessage)
      case e: TimeoutException => throw HttpException(Status.GatewayTimeout, e.getMessage)
      case e: Conflict => throw toHttpResponseException(e, Status.Conflict)
      case e: MissingInfo => throw toHttpResponseException(e, Status.UnprocessableEntity)
      case e: UnavailableAction => throw toHttpResponseException(e, Status.UnprocessableEntity)
      case e: UnknownProduct => throw BadRequestException(s"Product `${e.productName}` could not be found")
      case e: UnprocessableIntent => throw BadRequestException(e.getMessage)
      case e: RejectingError => throw BadRequestException(e.msg) // should not happen: every subclass of RejectingError should be covered above
    }
}
