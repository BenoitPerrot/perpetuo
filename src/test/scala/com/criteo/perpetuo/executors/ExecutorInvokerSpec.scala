package com.criteo.perpetuo.executors

import com.criteo.perpetuo.model.Operation.Operation
import com.twitter.inject.Test

import scala.concurrent.Future


class ExecutorInvokerSpec extends Test {
  class StupidExecutorInvoker(val substring: String) extends ExecutorInvoker {
    override def getExecutionDetailsUrlIfApplicable(uuid: String): Option[String] =
      Some(uuid).filter(_.contains(substring)).map(_ + s", because $substring!")

    override def trigger(operation: Operation, executionId: Long, productName: String, version: String, rawTarget: String, initiator: String): Option[Future[String]] = None
  }

  // Register two different ways of interpreting UUIDs
  new StupidExecutorInvoker("foo")
  new StupidExecutorInvoker("bar")


  "Getting execution details" should {
    "throw an exception" when {
      "the UUID is unintelligible" in {
        val thrown = the[Exception] thrownBy ExecutorInvoker.getExecutionDetailsUrl("blah")
        thrown.getMessage shouldEqual "Could not interpret UUID `blah`"
      }

      "the UUID is ambiguous" in {
        val thrown = the[Exception] thrownBy ExecutorInvoker.getExecutionDetailsUrl("foobar")
        thrown.getMessage should startWith("Multiple interpretations for `foobar`")
      }
    }

    "return the UUID immediately" when {
      "it's already a URL" in {
        ExecutorInvoker.getExecutionDetailsUrl("http://blah") shouldEqual "http://blah"
      }

      "it's already a URL (even when it could be understand by invokers)" in {
        ExecutorInvoker.getExecutionDetailsUrl("https://foo.bar") shouldEqual "https://foo.bar"
      }
    }

    "return the result of the interpretation" when {
      "exactly one invoker 'recognises' the UUID" in {
        ExecutorInvoker.getExecutionDetailsUrl("τ-foo") shouldEqual "τ-foo, because foo!"
        ExecutorInvoker.getExecutionDetailsUrl("barracuda") shouldEqual "barracuda, because bar!"
      }
    }
  }
}
