package com.criteo.perpetuo.util

import java.util.concurrent.ExecutionException

import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.UncheckedExecutionException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class FutureLoadingCache[A, B](cache: LoadingCache[A, Future[B]]) {
  def get(key: A): Future[B] = {
    val current = cache.getIfPresent(key)
    val res = if (current == null)
      cache.get(key)
    else {
      if (current.value.exists(_.isFailure)) { // it's already completed as failed, we know we can discard it
        cache.invalidate(key) // there might be a race, the risk is to invalidate a new value
        cache.get(key) // the current value wasn't new anyway, let's get a fresh one
      }
      else
        current
    }
    res.recover {
      case e@(_: ExecutionException | _: UncheckedExecutionException) => throw e.getCause
    }
  }
}
