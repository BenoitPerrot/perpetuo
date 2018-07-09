package com.criteo.perpetuo.auth

import java.util.regex.Pattern

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.model.Operation
import com.twitter.inject.Logging
import com.typesafe.config.Config

case class Authority(authorizedUserNames: Set[String],
                     authorizedGroupNames: Set[String]) {
  def authorizes(user: User): Boolean =
    authorizedUserNames.contains(user.name) || user.groupNames.intersect(authorizedGroupNames).nonEmpty
}

case class ProductRule(productPattern: Pattern, actionRules: Map[DeploymentAction.Value, Authority]) {
  def authorizes(user: User, action: DeploymentAction.Value, productName: String): Boolean =
    productPattern.matcher(productName).matches() && actionRules.get(action).exists(_.authorizes(user))
}

class FineGrainedPermissions(generalActionRules: Map[GeneralAction.Value, Authority], productRules: Seq[ProductRule]) extends Permissions {
  val permittedGroupNames: Set[String] = (
    generalActionRules.values.flatMap(_.authorizedGroupNames) ++
      productRules.flatMap(_.actionRules.values.flatMap(_.authorizedGroupNames))
    ).toSet

  override def isAuthorized(user: User, action: GeneralAction.Value): Boolean =
    generalActionRules.get(action).exists(_.authorizes(user))

  override def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String): Boolean =
    action match {
      case DeploymentAction.stopOperation =>
        productRules.exists(_.authorizes(user, DeploymentAction.applyOperation, productName))
      case _ =>
        productRules.exists(_.authorizes(user, action, productName))
    }
}

object FineGrainedPermissions extends Logging {
  private def createAuthority(config: Config): Authority =
    Authority(
      config.tryGetStringList("userNames").getOrElse(Set()).toSet,
      config.tryGetStringList("groupNames").getOrElse(Set()).toSet
    )

  private def createProductRule(config: Config): ProductRule =
    ProductRule(
      Pattern.compile(config.getString("regex")),
      createActionToAuthorityMap(DeploymentAction.values, config.getConfig("perAction"))
    )

  private def createActionToAuthorityMap[T](actions: Iterable[T], config: Config): Map[T, Authority] =
    actions.flatMap(a =>
      config.tryGetConfig(a.toString).map(authorityConfig =>
        a -> createAuthority(authorityConfig)
      )
    ).toMap

  def fromConfig(config: Config): FineGrainedPermissions = {
    val generalActionRules = config.tryGetConfig("perGeneralAction").map(perActionConfig =>
      createActionToAuthorityMap(GeneralAction.values, perActionConfig)
    ).getOrElse(Map())
    if (generalActionRules.isEmpty) {
      logger.warn("Nobody is authorized to perform general actions (e.g. administering)")
    }
    val productRules = config.tryGetConfigList("perProduct").map(_.map { perProductConfig =>
      createProductRule(perProductConfig)
    }).getOrElse(Seq())
    if (productRules.isEmpty) {
      logger.warn("Nobody is authorized to perform deployment-related actions (e.g. requesting deployments)")
    }
    new FineGrainedPermissions(generalActionRules, productRules)
  }
}
