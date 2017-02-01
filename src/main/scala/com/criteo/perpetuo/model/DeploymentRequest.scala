package com.criteo.perpetuo.model

import com.criteo.perpetuo.dispatchers.TargetExpr
import spray.json._

case class DeploymentRequest(id: Option[Long],
                             productId: Int,
                             version: String,
                             target: String,
                             comment: String, // Not an `Option` because it's easier to consider that no comment <=> empty
                             creator: String,
                             creationDate: java.sql.Timestamp) {

  // laziness of parsedTarget is handled by hand, to be able to duplicate the instance (see `setId`)
  // and still benefit from an already parsed target without forcing it
  private var parsedTargetCache: Option[TargetExpr] = None
  def parsedTarget: TargetExpr = parsedTargetCache.getOrElse {
    val parsed = DeploymentRequestParser.parseTargetExpression(target.parseJson)
    parsedTargetCache = Some(parsed)
    parsed
  }

  def copyWithId(actualId: Long): DeploymentRequest = {
    require(id.isEmpty)
    val clone = copy(id = Some(actualId))
    clone.parsedTargetCache = parsedTargetCache
    clone
  }
}