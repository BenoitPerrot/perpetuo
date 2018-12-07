package com.criteo.perpetuo.engine

import java.util.concurrent.TimeUnit

import com.criteo.perpetuo.SimpleScenarioTesting

class SchedulerSpec extends SimpleScenarioTesting {

  val scheduler = new Scheduler()

  test("Testing schedule at fixed rate") {
    var count: Int = 0

    val f = scheduler.scheduleTask(() => {count += 1}, period = 100, timeUnit = TimeUnit.MILLISECONDS)
    Thread.sleep(250)
    f.cancel(true)
    count shouldEqual 3
  }

}
