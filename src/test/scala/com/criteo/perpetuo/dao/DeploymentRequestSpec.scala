package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}
import slick.driver.JdbcDriver

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class DeploymentRequestSpec extends FunSuite with ScalaFutures
  with DeploymentRequestBinder
  with ProfileProvider {

  implicit val defaultPatience = PatienceConfig(timeout = Span(2, Seconds), interval = Span(100, Millis))

  private val config: Config = ConfigFactory.load()
  private val dbModule = new TestingDbContextModule(config.getConfig("db").getConfig("embedded"))

  val profile: JdbcDriver = dbModule.driver
  import profile.api._

  private val db = Database.forDataSource(dbModule.dataSourceProvider)
  private val schemaCreation = DBIO.seq(
    deploymentRequestQuery.schema.create
  )
  Await.result(db.run(schemaCreation), 2.second)

  test("Deployment requests can be inserted and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(for {
      id <- insert(db, request)
      requests <- db.run(deploymentRequestQuery.result)
      lookup <- findDeploymentRequestById(db, id)
    } yield {
      assert(requests.nonEmpty)
      assert(lookup.isDefined)
      assert(lookup.get == request.copy(id = Some(id)))
    }, 2.seconds)
  }
}
