package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.Crankshaft
import com.criteo.perpetuo.model.DeploymentRequest
import com.google.inject.{Inject, Provider, Singleton}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Provides a simplified, limited view of the Engine
  */
@Singleton
class EngineProxy @Inject()(crankshaftProvider: Provider[Crankshaft]) {

  private lazy val crankshaft = crankshaftProvider.get // To break the dependency cycle between Plugins and Engine

  def appendComment(deploymentRequest: DeploymentRequest, comment: String): Unit =
    Await.result(
      crankshaft.dbBinding.updateDeploymentRequestComment(
        deploymentRequest.id,
        s"${if (deploymentRequest.comment.nonEmpty) s"${deploymentRequest.comment}\n" else ""}$comment"
      ), 2.seconds
    )

  def readComment(deploymentRequest: DeploymentRequest): String =
    Await.result(crankshaft.dbBinding.findDeploymentRequestById(deploymentRequest.id).map(_.get.comment), 2.seconds)

}
