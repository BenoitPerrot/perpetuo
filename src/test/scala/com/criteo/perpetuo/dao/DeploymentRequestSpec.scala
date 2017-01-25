package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.ExecutionContext.Implicits.global


@RunWith(classOf[JUnitRunner])
class DeploymentRequestSpec extends FunSuite with ScalaFutures
  with DeploymentRequestBinder
  with TestDb
  with Eventually {

  implicit val defaultPatience = PatienceConfig(timeout = Span(1, Seconds), interval = Span(100, Millis))

  import dbContext.driver.api._

  test("Deployment requests can be inserted and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    eventually {
      for {
        id <- insert(request)
        requests <- dbContext.db.run(deploymentRequestQuery.result)
        lookup <- findDeploymentRequestById(id)
      } yield {
        assert(requests.nonEmpty)
        assert(lookup.isDefined)
        assert(lookup.get == request.copy(id = Some(id)))
      }
    }
  }
}
