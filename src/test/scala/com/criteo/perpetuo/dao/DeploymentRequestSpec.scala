package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.{ProtoDeploymentPlanStep, ProtoDeploymentRequest, Version}
import com.criteo.perpetuo.{TestDb, TestHelpers}
import spray.json.JsString

import scala.concurrent.ExecutionContext.Implicits.global


class DeploymentRequestSpec
  extends TestHelpers
    with DeploymentRequestBinder
    with ProductBinder
    with DeploymentRequestInserter
    with TestDb {

  import dbContext.profile.api._

  test("Deployment requests can be inserted and retrieved") {
    await(
      for {
        product <- insertProductIfNotExists("perpetuo-app")
        request <- insertDeploymentRequest(ProtoDeploymentRequest(product.name, Version("\"v42\""), Seq(ProtoDeploymentPlanStep("", JsString("*"), "")), "No fear", "c.norris")).map(_.deploymentRequest)
        requests <- dbContext.db.run(deploymentRequestQuery.result)
        lookup <- findDeploymentRequestById(request.id)
      } yield {
        requests shouldNot be(empty)
        lookup shouldBe defined
        lookup.get shouldEqual request
      }
    )
  }
}
