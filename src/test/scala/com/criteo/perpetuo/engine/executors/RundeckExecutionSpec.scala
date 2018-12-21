package com.criteo.perpetuo.engine.executors

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.model.ExecutionState
import com.criteo.perpetuo.util.ConsumedResponse
import com.twitter.conversions.time._
import com.twitter.finagle.http.{Request, Status}
import com.twitter.inject.Test
import com.twitter.io.Buf.Utf8
import com.twitter.util.{Duration, Future}
import org.scalatest.mockito.MockitoSugar
import spray.json.JsonParser.ParsingException


class RundeckExecutionSpec extends Test with MockitoSugar {

  private val rundeckConfig = AppConfig.executorConfig("rundeck")

  private class RundeckClientMock(contentMock: String, statusMock: Int) extends RundeckClient(rundeckConfig, "localhost") {
    def this(abortStatus: String, executionStatus: String, eventuallyCompleted: Boolean = false) =
      this(s"""{"abort": {"status": "$abortStatus"}, "execution": {"status": "$executionStatus"}, "execCompleted": $eventuallyCompleted}""", 200)

    override protected val baseWaitInterval: Duration = 1.millisecond
    override protected val jobTerminationTimeout: Duration = 10.milliseconds
    override protected val client: Request => Future[ConsumedResponse] =
      (_: Request) => Future.value(ConsumedResponse(Status(statusMock), Utf8(contentMock), "rundeck"))
    override protected val clientForIdempotentRequests: Request => Future[ConsumedResponse] = client
  }

  class RundeckExecutionWithClientMock(override val client: RundeckClient) extends RundeckExecution(rundeckConfig, "https://rundeck.criteo/project/my-project/execute/show/54")

  test("A RundeckExecution is built from a the permalink returned by Rundeck on trigger") {
    val remote = new RundeckExecution(rundeckConfig, "https://rundeck.criteo/project/my-project/executcriteon/show/42")
    remote.host shouldEqual "rundeck.criteo"
    remote.executionNumber shouldEqual 42

    val local = new RundeckExecution(rundeckConfig, "http://localhost:4440/project/my-project/execution/show/51")
    local.host shouldEqual "localhost"
    local.executionNumber shouldEqual 51
  }

  test("If a href makes no sense to the assigned execution factory, the instantiation must behave appropriately") {
    val exc = the[IllegalArgumentException] thrownBy new RundeckExecution(rundeckConfig, "http://rundeck.criteo/menu/home")
    exc.getMessage shouldEqual "Cannot find a proper Rundeck executor from http://rundeck.criteo/menu/home"
  }

  test("A running job can be aborted") {
    val executionAborted = new RundeckExecutionWithClientMock(new RundeckClientMock("aborted", "aborted"))
    executionAborted.stopper.get() shouldEqual None
  }

  test("When an abort is pending, wait for its completion") {
    val executionAborting = new RundeckExecutionWithClientMock(new RundeckClientMock("pending", "running", eventuallyCompleted = true))
    executionAborting.stopper.get() shouldEqual None
  }

  test("Aborting a running job is a failure if this one is still running after a timeout") {
    val execution = new RundeckExecutionWithClientMock(new RundeckClientMock("pending", "running"))
    execution.stopper.get() shouldEqual Some(ExecutionState.running)
  }

  test("Aborting a not running job is a success") {
    val executionFailed = new RundeckExecutionWithClientMock(new RundeckClientMock("failed", "failed"))
    executionFailed.stopper.get() shouldEqual None
    val executionCompleted = new RundeckExecutionWithClientMock(new RundeckClientMock("failed", "completed"))
    executionCompleted.stopper.get() shouldEqual None
    val executionAborted = new RundeckExecutionWithClientMock(new RundeckClientMock("failed", "aborted"))
    executionAborted.stopper.get() shouldEqual None
  }

  test("A job unknown from Rundeck is considered as unreachable") {
    val execution = new RundeckExecutionWithClientMock(new RundeckClientMock("{}", 404))
    execution.stopper.get() shouldEqual Some(ExecutionState.unreachable)
  }

  test("Raising an exception in case Rundeck doesn't answer or with an error different than 404") {
    val executionNotJson = new RundeckExecutionWithClientMock(new RundeckClientMock("<NOT JSON CODE>", 200))
    a[ParsingException] shouldBe thrownBy(executionNotJson.stopper.get())

    val executionForbidden = new RundeckExecutionWithClientMock(new RundeckClientMock("", 403))
    a[RuntimeException] shouldBe thrownBy(executionForbidden.stopper.get())
  }
}
