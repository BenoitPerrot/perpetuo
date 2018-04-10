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
import scala.io.Source


object TestUtils {
  def getResourceAsString(resourceFileName: String): String = {
    val cls = Class.forName(Thread.currentThread.getStackTrace.apply(2).getClassName)
    Source.fromURL(cls.getResource(resourceFileName)).mkString
  }
}


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

  def become[T](value: T): Matcher[Future[T]] = eventually(be(value))

  def eventually[T](matcher: Matcher[T]): Matcher[Future[T]] = matcher.compose(await)

  def await[T](a: Awaitable[T]): T = Await.result(a, 1.second)

  def deploy(productName: String, version: String, target: Seq[String], finalStatus: Status.Code = Status.success): ShallowOperationTrace = {
    if (!lastDeploymentRequests.contains(productName))
      await(engine.insertProduct(productName))

    val attrs = new DeploymentRequestAttrs(productName, Version(version.toJson), target.toJson.compactPrint, "", "de.ployer", new Timestamp(System.currentTimeMillis))
    await {
      for {
        depReqId <- engine.createDeploymentRequest(attrs).map { output =>
          val id = output("id").asInstanceOf[Long]
          lastDeploymentRequests(productName) = id
          id
        }
        op <- engine.startDeploymentRequest(depReqId, "s.tarter")
        operationTrace = op.get
        executionTraces <- engine.dbBinding.findExecutionTracesByOperationTrace(operationTrace.id)
        updated <- Future.traverse(executionTraces) { executionTrace =>
          engine.updateExecutionTrace(
            executionTrace.id, ExecutionState.completed, "", None,
            target.map(_ -> TargetAtomStatus(finalStatus, "")).toMap
          )
        }
      } yield {
        updated.foreach(_ shouldBe defined)
        operationTrace
      }
    }
  }

  def revert(productName: String, defaultVersion: Option[String] = None): ShallowOperationTrace = {
    val depReqId = lastDeploymentRequests(productName)
    await {
      for {
        op <- engine.revert(depReqId, "r.everter", defaultVersion.map(v => Version(v.toJson)))
        operationTrace = op.get
        executionIds <- engine.dbBinding.findExecutionIdsByOperationTrace(operationTrace.id)
        updated <- Future.traverse(executionIds) { executionId =>
          engine.dbBinding.findTargetsByExecution(executionId).flatMap { atoms =>
            engine.dbBinding.findExecutionTraceIdsByExecution(executionId).flatMap(executionTraceIds =>
              Future.traverse(executionTraceIds) { executionTraceId =>
                engine.updateExecutionTrace(
                  executionTraceId, ExecutionState.completed, "", None,
                  atoms.map(_ -> TargetAtomStatus(Status.success, "")).toMap
                )
              }
            )
          }
        }
      } yield {
        updated.foreach(_.foreach(_ shouldBe defined))
        operationTrace
      }
    }
  }
}
