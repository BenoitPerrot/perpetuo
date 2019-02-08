package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.util.{ConsumedResponse, SingleNodeHttpClient}
import com.twitter.conversions.time._
import com.twitter.finagle.http.{Request, Status}
import com.twitter.inject.Test
import com.twitter.io.Buf.Utf8
import com.twitter.util.{Duration, Future}
import org.scalatest.mockito.MockitoSugar
import spray.json.JsonParser.ParsingException


class RundeckExecutionSpec extends Test with MockitoSugar {
  private val href = "https://rundeck.criteo/project/my-project/execute/show/54"
  private val (rundeckHost, executionNumber) = RundeckExecution.parseHref(href)

  private def toExecution(abortStatus: String, executionStatus: String, eventuallyCompleted: Boolean = false): RundeckExecution =
    toExecution(200, s"""{"abort": {"status": "$abortStatus"}, "execution": {"status": "$executionStatus"}, "execCompleted": $eventuallyCompleted}""")

  private def toExecution(statusMock: Int, contentMock: String): RundeckExecution = {
    val clientMock = new SingleNodeHttpClient(rundeckHost, Duration.Top) {
      override def apply(request: Request, isIdempotent: Boolean = false): Future[ConsumedResponse] =
        Future.value(ConsumedResponse(Status(statusMock), Utf8(contentMock), "rundeck"))
    }
    val rundeckClient = new RundeckClient(clientMock, None) {
      override protected val baseWaitInterval: Duration = 1.millisecond
      override protected val terminationGlobalTimeout: Duration = 1.second
    }
    new RundeckExecution(rundeckClient, executionNumber, href)
  }

  test("RundeckExecution successfully parses a valid href") {
    val (host1, execNumber1) = RundeckExecution.parseHref("https://rundeck.criteo/project/my-project/executcriteon/show/42")
    host1 shouldEqual "rundeck.criteo"
    execNumber1 shouldEqual 42

    val (host2, execNumber2) = RundeckExecution.parseHref("http://localhost:4440/project/my-project/execution/show/51")
    host2 shouldEqual "localhost"
    execNumber2 shouldEqual 51
  }

  test("If an href makes no sense to RundeckExecution, it fails accordingly") {
    val exc = the[IllegalArgumentException] thrownBy RundeckExecution.parseHref("http://rundeck.criteo/menu/home")
    exc.getMessage shouldEqual "Cannot find a proper Rundeck executor from http://rundeck.criteo/menu/home"
  }

  test("A running job can be aborted") {
    val executionAborted = toExecution("aborted", "aborted")
    executionAborted.stopper.get() shouldEqual None
  }

  test("When an abort is pending, wait for its completion") {
    val executionAborting = toExecution("pending", "running", eventuallyCompleted = true)
    executionAborting.stopper.get() shouldEqual None
  }

  test("Aborting a running job is a failure if this one is still running after a timeout") {
    val execution = toExecution("pending", "running")
    execution.stopper.get() shouldEqual Some(ExecutionState.running)
  }

  test("Aborting a not running job is a success") {
    val executionFailed = toExecution("failed", "failed")
    executionFailed.stopper.get() shouldEqual None
    val executionCompleted = toExecution("failed", "completed")
    executionCompleted.stopper.get() shouldEqual None
    val executionAborted = toExecution("failed", "aborted")
    executionAborted.stopper.get() shouldEqual None
  }

  test("A job unknown from Rundeck is considered as unreachable") {
    val execution = toExecution(404, "{}")
    execution.stopper.get() shouldEqual Some(ExecutionState.unreachable)
  }

  test("Raising an exception in case Rundeck doesn't answer or with an error different than 404") {
    val executionNotJson = toExecution(200, "<NOT JSON CODE>")
    a[ParsingException] shouldBe thrownBy(executionNotJson.stopper.get())

    val executionForbidden = toExecution(403, "")
    a[RuntimeException] shouldBe thrownBy(executionForbidden.stopper.get())
  }
}
