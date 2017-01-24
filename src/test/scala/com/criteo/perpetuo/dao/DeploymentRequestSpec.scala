package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.app.{AppConfig, DbContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class DeploymentRequestSpec extends FunSuite with ScalaFutures
  with DeploymentRequestBinder
  with DbContextProvider {

  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  private val dbModule = new TestingDbContextModule(AppConfig.withEnv("test").db)
  val dbContext: DbContext = dbModule.providesDbContext
  import dbContext.driver.api._

  test("Deployment requests can be inserted and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(for {
      id <- insert(request)
      requests <- dbContext.db.run(deploymentRequestQuery.result)
      lookup <- findDeploymentRequestById(id)
    } yield {
      assert(requests.nonEmpty)
      assert(lookup.isDefined)
      assert(lookup.get == request.copy(id = Some(id)))
    }, 2.seconds)
  }
}
