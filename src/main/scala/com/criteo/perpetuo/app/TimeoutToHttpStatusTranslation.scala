package com.criteo.perpetuo.app

import com.twitter.finagle.http.Status.GatewayTimeout
import com.twitter.finatra.http.exceptions.HttpException

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, TimeoutException}

trait TimeoutToHttpStatusTranslation {
  def await[T](future: Future[T], maxDuration: Duration): T =
    handleTimeout(Await.result(future, maxDuration))

  def handleTimeout[T](action: => T): T =
    try {
      action
    }
    catch {
      case e: TimeoutException => throw HttpException(GatewayTimeout, e.getMessage)
    }
}
