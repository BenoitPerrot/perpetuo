package com.criteo.perpetuo.engine.invokers

import com.criteo.perpetuo.engine.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Test

import scala.concurrent.Future


class ExecutorInvokerSpec extends Test {
  class StupidExecutorInvoker(val substring: String) extends ExecutorInvoker {
    override def getExecutionDetailsUrlIfApplicable(logHref: String): Option[String] =
      Some(logHref).filter(_.contains(substring)).map(_ + s", because $substring!")

    override def trigger(execTraceId: Long, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = Future.successful(None)
  }

  // Register two different ways of interpreting log hrefs
  new StupidExecutorInvoker("foo")
  new StupidExecutorInvoker("bar")


  test("Getting execution details throws an exception when the log href is unintelligible") {
    val thrown = the[Exception] thrownBy ExecutorInvoker.getExecutionDetailsUrl("blah")
    thrown.getMessage shouldEqual "Could not interpret log href `blah`"
  }

  test("Getting execution details throws an exception when the log href is ambiguous") {
    val thrown = the[Exception] thrownBy ExecutorInvoker.getExecutionDetailsUrl("foobar")
    thrown.getMessage should startWith("Multiple interpretations for `foobar`")
  }

  test("Getting execution details returns the log href immediately when it's already a URL") {
    ExecutorInvoker.getExecutionDetailsUrl("http://blah") shouldEqual "http://blah"
  }

  test("Getting execution details returns the log href immediately when it's already a URL (even when it could be understand by invokers)") {
    ExecutorInvoker.getExecutionDetailsUrl("https://foo.bar") shouldEqual "https://foo.bar"
  }

  test("Getting execution details returns the result of the interpretation when exactly one invoker 'recognises' the log href") {
    ExecutorInvoker.getExecutionDetailsUrl("τ-foo") shouldEqual "τ-foo, because foo!"
    ExecutorInvoker.getExecutionDetailsUrl("barracuda") shouldEqual "barracuda, because bar!"
  }
}
