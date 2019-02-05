package com.criteo.perpetuo.util

import com.twitter.util.{Future => TwitterFuture, Promise => TwitterPromise, _}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise}
import scala.language.implicitConversions
import scala.util.{Failure, Success}


object FutureConversions {
  implicit def asScalaFuture[T](future: TwitterFuture[T]): ScalaFuture[T] = {
    val promise = ScalaPromise[T]()
    future.respond {
      case Return(resp) => promise.success(resp)
      case Throw(e) => promise.failure(e)
    }
    promise.future
  }

  implicit def asTwitterFuture[T](future: ScalaFuture[T]): TwitterFuture[T] = {
    val promise = TwitterPromise[T]()
    future.onComplete {
      case Success(resp) =>
        promise.setValue(resp)
      case Failure(e) =>
        promise.setException(e)
    }
    promise
  }
}
