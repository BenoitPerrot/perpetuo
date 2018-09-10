package com.criteo.perpetuo.auth

import java.util.regex.Pattern

import com.criteo.perpetuo.model.{Operation, TargetAtom, TargetAtomSet}
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
    val authorizedToStop = User("some.stopper")
    val authorizedToAdministrateInPreProd = User("preprod.admin")
    val authorizedToRequestInPreProd = User("preprod.req")
    val unauthorized = User("anonymous")
  }

  Map(
    "hand built" -> new FineGrainedPermissions(
      Map[GeneralAction.Value, Authority](
        GeneralAction.administrate -> Authority(Set(Users.authorizedToAdministrate.name), Set())
      ),
      Seq(
        new ProductRule(
          Pattern.compile(".*"),
          Map(
            DeploymentAction.requestOperation -> Iterable((
              Authority(Set(), Set(UserGroups.registeredUsers)),
              TargetMatchers(Seq())
            ))
          )
        ),
        new ProductRule(
          Pattern.compile(Products.foo.toString),
          Map(
            DeploymentAction.applyOperation -> Iterable((
              Authority(
                Set(Users.authorizedToProceedOnFoo.name),
                Set(UserGroups.hipsters, UserGroups.generationY)
              ),
              TargetMatchers(Seq())
            ))
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
      permissions.isAuthorized(Users.authorizedToAdministrate, GeneralAction.updateProduct) shouldBe false
      GeneralAction.values.exists(permissions.isAuthorized(Users.unauthorized, _)) shouldBe false
    }

    test(s"$subject authorizes users by their name, on specific products") {
      Operation.values.forall(permissions.isAuthorized(Users.authorizedToProceedOnFoo, DeploymentAction.applyOperation, _, Products.foo, None)) shouldBe true
      Operation.values.forall(permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.requestOperation, _, Products.foo, None)) shouldBe true
      Operation.values.forall(permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.requestOperation, _, Products.bar, None)) shouldBe true
    }

    test(s"$subject forbids users by their name, on specific products") {
      Operation.values.exists(permissions.isAuthorized(Users.authorizedToProceedOnFoo, DeploymentAction.applyOperation, _, Products.bar, None)) shouldBe false
      Products.values.exists(product =>
        DeploymentAction.values.exists(a =>
          Operation.values.exists(op =>
            permissions.isAuthorized(Users.unauthorized, a, op, product.toString, None)
          )
        )
      ) shouldBe false
    }

    test(s"$subject collects relevant group names") {
      permissions.permittedGroupNames shouldBe Set(UserGroups.registeredUsers, UserGroups.hipsters, UserGroups.generationY)
    }
  }

  test("Authorizes users by target using hand built permissions") {
    val permissions = new FineGrainedPermissions(
      Map[GeneralAction.Value, Authority](),
      Seq(
        new ProductRule(
          Pattern.compile(".*"),
          Map[DeploymentAction.Value, Iterable[(Authority, TargetMatchers)]](
            DeploymentAction.requestOperation ->
              Iterable(
                (Authority(Set(Users.authorizedToRequest.name), Set()), TargetMatchers(Seq((s: String) => Set("par", "pa4").contains(s)))),
                (Authority(Set(Users.authorizedToStop.name), Set()), TargetMatchers(Seq((s: String) => Set("par").contains(s))))
              ),
            DeploymentAction.applyOperation ->
              Iterable(
                (Authority(Set(Users.authorizedToAdministrate.name), Set(UserGroups.registeredUsers)), TargetMatchers(Seq((s: String) => Set("target", "anotherTarget").contains(s))))
              ),
            DeploymentAction.stopOperation ->
              Iterable(
                (Authority(Set(Users.authorizedToStop.name), Set()), TargetMatchers(Seq((s: String) => Set("par").contains(s))))
              )
          )
        )
      )
    )

    permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.requestOperation, Operation.deploy, "product1",
      Some(TargetAtomSet(Set(TargetAtom("par"))))) shouldBe true
    permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.requestOperation, Operation.deploy, "product1",
      Some(TargetAtomSet(Set(TargetAtom("am5"))))) shouldBe false
    permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.requestOperation, Operation.deploy, "product1",
      Some(TargetAtomSet(Set(TargetAtom("par"), TargetAtom("pa4"), TargetAtom("am5"))))) shouldBe false
    permissions.isAuthorized(Users.unauthorized, DeploymentAction.requestOperation, Operation.deploy, "product1",
      Some(TargetAtomSet(Set(TargetAtom("pa3"))))) shouldBe false

    permissions.isAuthorized(Users.authorizedToAdministrate, DeploymentAction.applyOperation, Operation.deploy, "product1",
      Some(TargetAtomSet(Set(TargetAtom("target"))))) shouldBe true
    permissions.isAuthorized(Users.authorizedToRequest, DeploymentAction.applyOperation, Operation.deploy, "product1",
      Some(TargetAtomSet(Set(TargetAtom("anotherTarget"))))) shouldBe true

    permissions.isAuthorized(Users.authorizedToStop, DeploymentAction.requestOperation, Operation.deploy, "product2",
      Some(TargetAtomSet(Set(TargetAtom("par"))))) shouldBe true
  }

  test("Authorizes users by target using permissions from config") {
    val permissions = FineGrainedPermissions.fromConfig(ConfigFactory.parseString(
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
         |        },
         |        {
         |          targetMatchers {
         |            atoms = ["par"]
         |          },
         |          userNames = ["${Users.authorizedToRequestInPreProd.name}"]
         |        }
         |      ]
         |    }
         |  }
         |]
        """.stripMargin))

    permissions.isAuthorized(Users.authorizedToAdministrate, GeneralAction.administrate) shouldBe true
    permissions.isAuthorized(Users.authorizedToRequestInPreProd, DeploymentAction.requestOperation, Operation.deploy, "product", Some(TargetAtomSet(Set(TargetAtom("par"))))) shouldBe true
    permissions.isAuthorized(Users.authorizedToRequestInPreProd, DeploymentAction.requestOperation, Operation.deploy, "product", Some(TargetAtomSet(Set(TargetAtom("am5"))))) shouldBe false
    permissions.isAuthorized(Users.authorizedToStop, DeploymentAction.requestOperation, Operation.deploy, "product", Some(TargetAtomSet(Set(TargetAtom("par"))))) shouldBe false
  }

}
