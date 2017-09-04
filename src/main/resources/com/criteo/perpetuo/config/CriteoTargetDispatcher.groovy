package com.criteo.perpetuo.config

import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.engine.executors.RundeckInvoker
import com.criteo.perpetuo.model.Version
import groovy.json.JsonOutput


/* the "public" class to be loaded as the actual plugin must be the first statement after the imports */

class CriteoTargetDispatcher extends TargetDispatcher {
    ExecutorInvoker invoker

    CriteoTargetDispatcher(RootAppConfig appConfig) {
        final def RUNDECK_API_VERSION = 16
        def env = appConfig.env()
        switch (env) {
            case 'local':
                def rundeckPort = (System.getenv("RD_PORT") ?: "4440")
                invoker = new RundeckInvoker("rundeck", "localhost", rundeckPort as int, RUNDECK_API_VERSION, appConfig.under('tokens').get('rundeck').toString())
                break
            default:
                invoker = new RundeckInvoker("rundeck", "rundeck.central.criteo.${env}", 443, RUNDECK_API_VERSION, appConfig.under('tokens').get('rundeck').toString())
        }
    }

    @Override
    Iterable<ExecutorInvoker> assign(String targetAtom) {
        return [invoker]
    }

    @Override
    String freezeParameters(String executionKind, String productName, Version version) {
        String jobName
        String uploaderVersion = null
        switch (productName) {
            case 'config-as-code':
                jobName = 'deploy-cac'
                break
            default:
                String productType = CriteoExternalData.fetchManifest(productName)?.get('type') ?: 'marathon'
                // fixme: only accept active products here (https://jira.criteois.com/browse/DREDD-309)
                jobName = "$executionKind-to-$productType"
                uploaderVersion = System.getenv("${productType.toUpperCase()}_UPLOADER")
                break
        }
        JsonOutput.toJson([
                jobName: jobName,
                uploaderVersion: uploaderVersion
        ].findAll { it.value })
    }
}
