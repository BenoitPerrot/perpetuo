package com.criteo.perpetuo.dao

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.{DeploymentRequest, Product}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

@RunWith(classOf[JUnitRunner])
class DeploymentRequestSpec extends FunSuite with ScalaFutures
  with DeploymentRequestBinder with ProductBinder
  with TestDb {

  import dbContext.driver.api._

  test("Deployment requests can be inserted and retrieved") {
    Await.result(
      for {
        request <- insert(Product(None, "perpetuo-app")).map { productId =>
          DeploymentRequest(None, productId, "v42", "*", "No fear", "c.norris", new Timestamp(123456789)) }
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
