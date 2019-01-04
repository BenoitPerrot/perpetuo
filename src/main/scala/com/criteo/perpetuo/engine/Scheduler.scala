package com.criteo.perpetuo.engine

import java.util.concurrent.{Executors, Future, TimeUnit}

import com.google.inject.Singleton

@Singleton
class Scheduler {
  private val THREAD_COUNT = 2 // TODO: Increase the thread count relatively to the number of tasks that need to be executed
  private val executor = Executors.newScheduledThreadPool(THREAD_COUNT)

  def scheduleTask(f: () => Any, period: Long, timeUnit: TimeUnit, initialDelay: Long = 0): Future[_] = {
    executor.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = f()
    }, initialDelay, period, timeUnit)
  }
}
