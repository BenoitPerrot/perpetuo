package com.criteo.perpetuo.model

import com.criteo.perpetuo.engine.TargetExpr

import scala.collection.JavaConverters._


object Target {
  def getSimpleSelect(target: TargetExpr): Iterable[String] = target.flatMap(_.select)

  def getSimpleSelectForGroovy(target: TargetExpr): java.util.Set[String] = target.flatMap(_.select).asJava
}


case class ProtoDeploymentRequest(productName: String,
                                  version: Version,
                                  plan: Seq[ProtoDeploymentPlanStep],
                                  comment: String,
                                  creator: String,
                                  creationDate: java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis)) {
  val target: String = plan.head.targetExpression.compactPrint
}


case class DeploymentRequest(id: Long,
                             product: Product,
                             version: Version,
                             comment: String,
                             creator: String,
                             creationDate: java.sql.Timestamp) {

  val productId: Int = product.id
}
