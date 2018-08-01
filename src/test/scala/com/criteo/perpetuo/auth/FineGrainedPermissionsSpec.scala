package com.criteo.perpetuo.auth

import java.util.regex.Pattern

import com.criteo.perpetuo.model.Operation
import com.twitter.inject.Test
import com.typesafe.config.ConfigFactory


/**
  * Test the [[FineGrainedPermissions]] system.
  *
  */
class FineGrainedPermissionsSpec extends Test {

  object Products extends Enumeration {
    val foo = "foo"
    val bar = "bar"
  }

  object UserGroups {
    val registeredUsers = "Registered Users"
    val hipsters ="Hipsters"
    val generationY = "Generation Y"
  }

  object Users {
    val authorizedToAdministrate = User("sub.admin")
    val authorizedToProceedOnFoo = User("foo.proceeder")
    val authorizedToRequest = User("some.one", Set(UserGroups.registeredUsers))
    val unauthorized = User("anonymous")
  }

  Map(
    "hand built" -> new FineGrainedPermissions(
      Map[GeneralAction.Value, Authority](
        GeneralAction.administrate -> Authority(Set(Users.authorizedToAdministrate.name), Set())
      ),
      Seq[ProductRule](
        ProductRule(
          Pattern.compile(".*"),
          Map(
            DeploymentAction.requestOperation -> Authority(Set(), Set(UserGroups.registeredUsers))
          )
        ),
        ProductRule(
          Pattern.compile(Products.foo.toString),
          Map(
            DeploymentAction.applyOperation -> Authority(
              Set(Users.authorizedToProceedOnFoo.name),
              Set(UserGroups.hipsters, UserGroups.generationY)
            )
          )
        )
      )
    ),
    "parsed from config" -> FineGrainedPermissions.fromConfig(ConfigFactory.parseString(
      s"""
        |perGeneralAction = {
        |  administrate = [
        |    {
        |      userNames = ["${Users.authorizedToAdministrate.name}"]
        |    }
        |  ]
        |}
        |
        |perProduct = [
        |  {
        |    regex = ".*"
        |    perAction {
        |      requestOperation = [
        |        {
        |          groupNames = ["${UserGroups.registeredUsers}"]
        |        }
        |      ]
        |    }
        |  },
        |  {
        |    regex = "${Products.foo}"
        |    perAction {
        |      applyOperation = [
        |        {
        |          userNames = ["${Users.authorizedToProceedOnFoo.name}"]
        |          groupNames = ["${UserGroups.hipsters}", "${UserGroups.generationY}"]
        |        }
        |      ]
        |    }
        |  }
        |]
        """.stripMargin))
  ).foreach { case (initMethod: String, permissions: FineGrainedPermissions) =>

    val subject = s"The FineGrainedPermissions system ($initMethod)"

    test(s"$subject authorizes users by their name on specific actions") {
      permissions.isAuthorized(Users.authorizedToAdministrate, GeneralAction.administrate)
    }

    test(s"$subject forbids users by their name on specific actions") {
      permissions.isAuthorized(Users.authorizedToAdministrate, GeneralAction.addProduct) shouldBe false
      GeneralAction.values.exists(permissions.isAuthorized(Users.unauthorized, _)) shouldBe false
    }

    test(s"$subject authorizes users by their name, on specific products") {
      Operation.values.forall(permissions.isAuthorized(Users.authorizedToProceedOnFoo, DeploymentAction.applyOperation, _, Products.foo)) shouldBe true
      Operation.values.forall(permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.requestOperation, _, Products.foo)) shouldBe true
      Operation.values.forall(permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.requestOperation, _, Products.bar)) shouldBe true
    }

    test(s"$subject forbids users by their name, on specific products") {
      Operation.values.exists(permissions.isAuthorized(Users.authorizedToProceedOnFoo, DeploymentAction.applyOperation, _, Products.bar)) shouldBe false
      Products.values.exists(product =>
        DeploymentAction.values.exists(a =>
          Operation.values.exists(op =>
            permissions.isAuthorized(Users.unauthorized, a, op, product.toString)
          )
        )
      ) shouldBe false
    }

    test(s"$subject collects relevant group names") {
      permissions.permittedGroupNames shouldBe Set(UserGroups.registeredUsers, UserGroups.hipsters, UserGroups.generationY)
    }
  }

}
