package com.criteo.perpetuo.auth

import java.util.regex.Pattern

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.model.Operation
import com.twitter.inject.Logging
import com.typesafe.config.Config

import scala.collection.JavaConversions._

case class Authority(authorizedUserNames: Set[String],
                     authorizedGroupNames: Set[String]) {
  def authorizes(user: User): Boolean =
    authorizedUserNames.contains(user.name) || user.groupNames.intersect(authorizedGroupNames).nonEmpty
}

case class TargetMatchers(matchers: Iterable[String => Boolean]) {
  def authorizes(targets: Iterable[String]): Boolean =
    matchers.isEmpty || targets.forall(target => matchers.exists(_(target)))
}

case class ProductRule(productPattern: Pattern, actionRules: Map[DeploymentAction.Value, Iterable[(Authority, TargetMatchers)]]) {
  def authorizes(user: User, action: DeploymentAction.Value, productName: String): Boolean =
    productPattern.matcher(productName).matches() && actionRules.get(action).exists(rules => rules.exists {
      case (authority, targetMatcher) => authority.authorizes(user) && targetMatcher.authorizes(Seq[String]()) // TODO: Pass the list of targets to target matcher
    })
}

class FineGrainedPermissions(generalActionRules: Map[GeneralAction.Value, Authority], productRules: Seq[ProductRule]) extends Permissions {
  val permittedGroupNames: Set[String] = (
    generalActionRules.values.flatMap(_.authorizedGroupNames) ++
      productRules.flatMap(_.actionRules.values.flatMap(_.flatMap { case (authority, _) => authority.authorizedGroupNames }))
    ).toSet

  override def isAuthorized(user: User, action: GeneralAction.Value): Boolean =
    generalActionRules.get(action).exists(_.authorizes(user))

  override def isAuthorized(user: User, action: DeploymentAction.Value, operation: Operation.Kind, productName: String): Boolean =
    productRules.exists(
      _.authorizes(user, if (action == DeploymentAction.stopOperation) DeploymentAction.applyOperation else action, productName)
    )
}

object FineGrainedPermissions extends Logging {

  private def createTargetsMatcher(config: Config): TargetMatchers =
    TargetMatchers(
      config.tryGetConfig("targetMatchers")
        .map(targetConfig =>
          targetConfig.entrySet
            .map(_.getKey).toList
            .map {
              case k@"atoms" => targetConfig.getStringList(k).toSet.contains _
              case f => throw new IllegalArgumentException(s"Permissions: Target matcher name $f not recognized")
            }
        ).getOrElse(Iterable[String => Boolean]())
    )

  private def createAuthority(config: Config): Authority =
    Authority(
      config.tryGetStringList("userNames").getOrElse(Set()).toSet,
      config.tryGetStringList("groupNames").getOrElse(Set()).toSet
    )

  private def createProductRule(config: Config): ProductRule =
    ProductRule(
      Pattern.compile(config.getString("regex")),
      createActionToAuthorityTargetMatchersMap(DeploymentAction.values, config.getConfig("perAction"))
    )

  private def createActionToAuthorityMap[T](actions: Iterable[T], config: Config): Map[T, Authority] =
    actions.flatMap(a =>
      config.tryGetConfigList(a.toString).getOrElse(Seq()).map(authorityConfig =>
        a -> createAuthority(authorityConfig))
    ).toMap

  private def createActionToAuthorityTargetMatchersMap[T](actions: Iterable[T], config: Config): Map[T, Iterable[(Authority, TargetMatchers)]] =
    actions.map(a =>
      a -> config.tryGetConfigList(a.toString).getOrElse(Seq()).map(actionConfig =>
        (createAuthority(actionConfig), createTargetsMatcher(actionConfig)))
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
