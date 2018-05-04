package com.criteo.perpetuo.dao

import com.criteo.perpetuo.TestDb
import com.criteo.perpetuo.model.{DeploymentRequestAttrs, ProtoDeploymentPlanStep, Version}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent._
import org.scalatest.junit.JUnitRunner
import spray.json.JsString

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class DeploymentRequestSpec
  extends FunSuite
    with ScalaFutures
    with DeploymentRequestBinder
    with ProductBinder
    with DeploymentRequestInserter
    with TestDb {

  import dbContext.driver.api._

  test("Deployment requests can be inserted and retrieved") {
    Await.result(
      for {
        product <- insertProduct("perpetuo-app")
        request <- insertDeploymentRequest(new DeploymentRequestAttrs(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "No fear", "c.norris"))
        requests <- dbContext.db.run(deploymentRequestQuery.result)
        lookup <- findDeepDeploymentRequestById(request.id)
      } yield {
        assert(requests.nonEmpty)
        assert(lookup.isDefined)
        assert(lookup.get == request)
      },
      1.second
    )
  }
}
