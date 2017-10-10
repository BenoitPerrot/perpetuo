package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.engine.engine.TargetExpr
import com.criteo.perpetuo.model.Version
import com.twitter.inject.Test

import scala.concurrent.Future


class ExecutorInvokerSpec extends Test {
  class StupidExecutorInvoker(val substring: String) extends ExecutorInvoker {
    override def getExecutionDetailsUrlIfApplicable(logHref: String): Option[String] =
      Some(logHref).filter(_.contains(substring)).map(_ + s", because $substring!")

    override def trigger(execTraceId: Long, executionKind: String, productName: String, version: Version, target: TargetExpr, initiator: String): Future[Option[String]] = Future.successful(None)
  }

  // Register two different ways of interpreting log hrefs
  new StupidExecutorInvoker("foo")
  new StupidExecutorInvoker("bar")


  "Getting execution details" should {
    "throw an exception" when {
      "the log href is unintelligible" in {
        val thrown = the[Exception] thrownBy ExecutorInvoker.getExecutionDetailsUrl("blah")
        thrown.getMessage shouldEqual "Could not interpret log href `blah`"
      }

      "the log href is ambiguous" in {
        val thrown = the[Exception] thrownBy ExecutorInvoker.getExecutionDetailsUrl("foobar")
        thrown.getMessage should startWith("Multiple interpretations for `foobar`")
      }
    }

    "return the log href immediately" when {
      "it's already a URL" in {
        ExecutorInvoker.getExecutionDetailsUrl("http://blah") shouldEqual "http://blah"
      }

      "it's already a URL (even when it could be understand by invokers)" in {
        ExecutorInvoker.getExecutionDetailsUrl("https://foo.bar") shouldEqual "https://foo.bar"
      }
    }

    "return the result of the interpretation" when {
      "exactly one invoker 'recognises' the log href" in {
        ExecutorInvoker.getExecutionDetailsUrl("τ-foo") shouldEqual "τ-foo, because foo!"
        ExecutorInvoker.getExecutionDetailsUrl("barracuda") shouldEqual "barracuda, because bar!"
      }
    }
  }
}
