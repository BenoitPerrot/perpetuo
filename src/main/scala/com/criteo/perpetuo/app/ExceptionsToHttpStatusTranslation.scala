package com.criteo.perpetuo.app

import com.criteo.perpetuo.engine.dispatchers.NoAvailableExecutor
import com.criteo.perpetuo.engine.{Conflict, MissingInfo, PermissionDenied, RejectingError, UnavailableAction, UnprocessableIntent, Veto}
import com.twitter.finagle.http.Status
import com.twitter.finagle.{TimeoutException => FinagleTimeout}
import com.twitter.finatra.http.exceptions._
import com.twitter.finatra.http.response.ResponseBuilder
import com.twitter.inject.Logging
import com.twitter.util.{TimeoutException => TwitterTimeout}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}


trait ExceptionsToHttpStatusTranslation extends Logging {
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
      case _: PermissionDenied => throw ForbiddenException()
      case e: Conflict => throw toHttpResponseException(e, Status.Conflict)
      case e: MissingInfo => throw toHttpResponseException(e, Status.UnprocessableEntity)
      case _: NoAvailableExecutor =>
        val msg = "No executor available to do the actual deployment work"
        logger.error(msg)
        throw ServiceUnavailableException(msg)
      case e@(_: TimeoutException | _: TwitterTimeout | _: FinagleTimeout) =>
        logger.error(e.getMessage, e)
        throw HttpException(Status.GatewayTimeout, e.getMessage)
      case e: UnavailableAction => throw toHttpResponseException(e, Status.UnprocessableEntity)
      case e: UnprocessableIntent => throw toHttpResponseException(e, Status.BadRequest)
      case e: Veto => throw toHttpResponseException(e, Status.UnprocessableEntity)
      case e: RejectingError => // should not happen: every subclass of RejectingError should be covered above
        logger.error(e.getMessage, e)
        throw toHttpResponseException(e, Status.BadRequest)
    }
}
