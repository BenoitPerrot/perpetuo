package com.criteo.perpetuo.model


case class ProtoDeploymentRequest(productName: String,
                                  version: Version,
                                  plan: Seq[ProtoDeploymentPlanStep],
                                  comment: String,
                                  creator: String,
                                  creationDate: java.sql.Timestamp = new java.sql.Timestamp(System.currentTimeMillis))


case class DeploymentRequest(id: Long,
                             product: Product,
                             version: Version,
                             comment: String,
                             creator: String,
                             creationDate: java.sql.Timestamp) {

  val productId: Int = product.id
}
