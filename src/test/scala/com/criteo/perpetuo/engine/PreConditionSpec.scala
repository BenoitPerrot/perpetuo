package com.criteo.perpetuo.engine

import com.criteo.perpetuo.SimpleScenarioTesting
import com.criteo.perpetuo.auth.{Permissions, User}
import com.criteo.perpetuo.config.{PluginLoader, TestConfig}
import com.criteo.perpetuo.model.{ProtoDeploymentPlanStep, ProtoDeploymentRequest, Version}
import com.typesafe.config.ConfigFactory
import spray.json.JsString

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global

class PreConditionSpec extends SimpleScenarioTesting {

  override def providesPermissions: Permissions =
    injector
      .getInstance(classOf[PluginLoader])
      .loadPermissions(
        Some(
          ConfigFactory
            .parseString(
              s"""
                 |permissions {
                 |  type = "fineGrained"
                 |  fineGrained {
                 |    perGeneralAction {
                 |      updateProduct = [
                 |        {
                 |          userNames = ["bob.the.producer"]
                 |        }
                 |      ]
                 |    }
                 |    perProduct = [
                 |      {
                 |        regex = ".*"
                 |        perAction {
                 |          requestOperation = [
                 |            {
                 |              groupNames = ["Users"]
                 |            }
                 |          ]
                 |        }
                 |      }
                 |    ]
                 |  }
                 |}""".stripMargin
            )
            .resolve()
            .getConfig("permissions")
        )
      )

  override def providesPreConditionEvaluators: Seq[AsyncPreConditionEvaluator] =
    injector
      .getInstance(classOf[PluginLoader])
      .loadPreConditionEvaluators(
        Seq(ConfigFactory.parseString(
            s"""
               |{
               |  type = "class"
               |  class = "com.criteo.perpetuo.engine.preconditions.TestPreCondition"
               |}
               |""".stripMargin))
      )

  val stdUser = new User("s.omeone", Set("Users"))
  val lonelyUser = new User("f.alone", Set())
  val producer = new User("bob.the.producer", Set("Users"))

  val nonRequestableProduct: String = "nonRequestableProduct"
  val mProduct: String = "mProduct"

  test("Cannot request a deployment when pre-conditions aren't met") {
    await(
      for {
        _ <- engine.upsertProduct(producer, nonRequestableProduct)
        _ <- engine.upsertProduct(producer, mProduct)
        r <- engine.requestDeployment(stdUser, ProtoDeploymentRequest(nonRequestableProduct, Version("1"), Seq(), "", stdUser.name)).failed
        r2 <- engine.requestDeployment(stdUser, ProtoDeploymentRequest(mProduct, Version("1"), Seq(), "", stdUser.name))
      } yield {
        r shouldBe a[PreConditionFailed]
        r2.product.name shouldEqual mProduct
      }
    )
  }

  test("Return PermissionDenied instead of PreConditionFailed") {
    await(
      for {
        _ <- engine.upsertProduct(producer, nonRequestableProduct)
        r <- engine.requestDeployment(lonelyUser, ProtoDeploymentRequest(nonRequestableProduct, Version("1"), Seq(), "", lonelyUser.name)).failed
      } yield {
        r shouldBe a[PermissionDenied]
      }
    )
  }

  test("Return Unidentified instead of PreConditionFailed") {
    await(
      for {
        _ <- engine.upsertProduct(producer, mProduct)
        depReq <- engine.requestDeployment(stdUser, ProtoDeploymentRequest(mProduct, Version("1"), Seq(ProtoDeploymentPlanStep("", JsString("atom"), "")), "", stdUser.name))
        r <- engine.queryDeploymentRequestStatus(None, depReq.id)
        m = r.get._2.map { case (action, permission) => action -> permission }.toMap
      } yield {
        r shouldBe defined
        m("deploy") shouldEqual Some("unidentified")
        m("abandon") shouldEqual Some("unidentified")
      }
    )
  }

}
