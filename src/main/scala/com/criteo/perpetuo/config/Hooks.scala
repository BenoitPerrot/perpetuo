package com.criteo.perpetuo.config

import com.criteo.perpetuo.model.DeploymentRequest


class Hooks {
  /**
    * Methods that can be overridden as hooks.
    */
  def onDeploymentRequestCreated(deploymentRequest: DeploymentRequest): Unit = {}

  def onDeploymentRequestStarted(deploymentRequest: DeploymentRequest, immediately: Boolean): Unit = {}
}
