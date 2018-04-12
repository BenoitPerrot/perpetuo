package com.criteo.perpetuo

import java.sql.Timestamp

import com.criteo.perpetuo.config.{AppConfigProvider, PluginLoader, Plugins}
import com.criteo.perpetuo.dao.{DbBinding, DbContext, DbContextProvider, TestingDbContextModule}
import com.criteo.perpetuo.engine.dispatchers.SingleTargetDispatcher
import com.criteo.perpetuo.engine.executors.{DummyExecutionTrigger, TriggeredExecutionFinder}
import com.criteo.perpetuo.engine.{Engine, TargetExpr}
import com.criteo.perpetuo.model._
import com.twitter.inject.Test
import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.scalatest.matchers.Matcher
import org.scalatest.mockito.MockitoSugar
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Awaitable, Future}
import scala.reflect.ClassTag


trait TestDb extends DbContextProvider {
  lazy val dbTestModule = new TestingDbContextModule(AppConfigProvider.config.getConfig("db"))
  lazy val dbContext: DbContext = dbTestModule.providesDbContext
  lazy val dbBinding = new DbBinding(dbContext)
}


trait SimpleScenarioTesting extends Test with TestDb with MockitoSugar {
  private val lastDeploymentRequests = mutable.Map[String, Long]()
  private val loader = new PluginLoader(null)
  private val executionTrigger: DummyExecutionTrigger = mock[DummyExecutionTrigger]
  val plugins = new Plugins(loader)
  val executionFinder = new TriggeredExecutionFinder(loader)
  lazy val engine: Engine = {
    when(executionTrigger.trigger(anyInt, anyString, any[Version], any[TargetExpr], anyString))
      .thenReturn(Future(triggerMock))
    new Engine(dbBinding, plugins.resolver, new SingleTargetDispatcher(executionTrigger), plugins.permissions, plugins.listeners, executionFinder)
  }

  protected def triggerMock: Option[String] = None

  def asynchronouslyThrow[T <: Throwable : ClassTag](pattern: String): Matcher[Future[_]] =
    be(a[T])
      .and(fullyMatch.regex(pattern).compose((_: Throwable).getMessage))
      .compose(f => await(f.failed))

  def become[T](value: T): Matcher[Future[T]] = eventually(be(value))

  def eventually[T](matcher: Matcher[T]): Matcher[Future[T]] = matcher.compose(await)

  def await[T](a: Awaitable[T]): T = Await.result(a, 1.second)

  def closeOperation(operationTrace: OperationTrace,
                     targetFinalStatus: Map[String, Status.Code] = Map(),
                     initFailed: Boolean = false): Future[Seq[Long]] =
    engine.dbBinding.findExecutionIdsByOperationTrace(operationTrace.id)
      .flatMap(executionIds =>
        Future.traverse(executionIds)(executionId =>
          targetFinalStatus.headOption
            .map(_ => Future.successful(targetFinalStatus))
            .getOrElse(engine.dbBinding.findTargetsByExecution(executionId).map(_.map(_ -> Status.success).toMap))
            .zip(engine.dbBinding.findExecutionTraceIdsByExecution(executionId))
            .flatMap { case (statusMap, executionTraceIds) =>
              val finalStatusMap = statusMap.mapValues(TargetAtomStatus(_, ""))
              val executionState = if (initFailed) ExecutionState.initFailed else ExecutionState.completed
              Future.traverse(executionTraceIds)(engine.updateExecutionTrace(_, executionState, "", None, finalStatusMap))
            }
        )
      )
      .map(_.flatten.map(_.getOrElse(throw new AssertionError("An execution trace has not been updated"))))

  def deploy(productName: String, version: String, target: Seq[String], finalStatus: Status.Code = Status.success): DeepOperationTrace = {
    if (!lastDeploymentRequests.contains(productName))
      await(engine.insertProduct(productName))

    val attrs = new DeploymentRequestAttrs(productName, Version(version.toJson), target.toJson.compactPrint, "", "de.ployer", new Timestamp(System.currentTimeMillis))
    await {
      for {
        depReqId <- engine.createDeploymentRequest(attrs).map {
          output =>
            val id = output("id").asInstanceOf[Long]
            lastDeploymentRequests(productName) = id
            id
        }
        op <- engine.startDeploymentRequest(depReqId, "s.tarter")
        operationTrace = op.get
        _ <- closeOperation(operationTrace, target.map(_ -> finalStatus).toMap)
      } yield operationTrace
    }
  }

  def revert(productName: String, defaultVersion: Option[String] = None): DeepOperationTrace = {
    val depReqId = lastDeploymentRequests(productName)
    await {
      for {
        op <- engine.revert(depReqId, "r.everter", defaultVersion.map(v => Version(v.toJson)))
        operationTrace = op.get
        _ <- closeOperation(operationTrace)
      } yield operationTrace
    }
  }
}
