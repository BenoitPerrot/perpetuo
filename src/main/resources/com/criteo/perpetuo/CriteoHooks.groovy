package com.criteo.perpetuo

import com.criteo.perpetuo.config.Hooks
import com.criteo.perpetuo.model.DeploymentRequest


class CriteoHooks extends Hooks {
    @Override
    void onDeploymentRequestCreated(DeploymentRequest deploymentRequest) {
        logger().info("DEPLOYMENT REQUEST CREATED")
    }
}
