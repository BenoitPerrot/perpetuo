package com.criteo.perpetuo.model

import java.sql.Timestamp

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.dao.{DeploymentRequestBinder, ProductBinder}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class DeploymentRequestSpec extends FunSuite with ScalaFutures
  with DeploymentRequestBinder with ProductBinder
  with TestDb {

  import dbContext.driver.api._

  test("Deployment requests can be inserted and retrieved") {
    Await.result(
      for {
        product <- insertProduct("perpetuo-app")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"v42\""), "*", "No fear", "c.norris", new Timestamp(123456789)))
        requests <- dbContext.db.run(deploymentRequestQuery.result)
        lookup <- findDeploymentRequestById(request.id)
      } yield {
        assert(requests.nonEmpty)
        assert(lookup.isDefined)
        assert(lookup.get == request)
      },
      1.second
    )
  }
}
