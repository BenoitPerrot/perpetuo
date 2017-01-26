package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.DeploymentRequest
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class DeploymentRequestSpec extends FunSuite with ScalaFutures
  with DeploymentRequestBinder
  with TestDb {

  import dbContext.driver.api._

  test("Deployment requests can be inserted and retrieved") {
    val request = DeploymentRequest(None, "perpetuo-app", "v42", "*", "No fear", "c.norris", new Timestamp(123456789))

    Await.result(
      for {
        id <- insert(request)
        requests <- dbContext.db.run(deploymentRequestQuery.result)
        lookup <- findDeploymentRequestById(id)
      } yield {
        assert(requests.nonEmpty)
        assert(lookup.isDefined)
        assert(lookup.get == request.copy(id = Some(id)))
      },
      1.second
    )
  }
}
