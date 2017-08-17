package com.criteo.perpetuo.config

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.DummyInvoker
import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.engine.executors.HttpInvoker
import com.criteo.perpetuo.model.Version
import com.twitter.finagle.http.Fields
import com.twitter.finagle.http.Message$
import com.twitter.finagle.http.Method
import com.twitter.finagle.http.Request
import groovy.json.JsonBuilder
import groovy.json.JsonException
import groovy.json.JsonOutput
import groovy.json.JsonSlurper


/* the "public" class to be loaded as the actual plugin must be the first statement after the imports */

class CriteoTargetDispatcher extends TargetDispatcher {
    ExecutorInvoker invoker

    CriteoTargetDispatcher(DbBinding dbBinding, RootAppConfig appConfig) {
        def env = appConfig.env()
        switch (env) {
            case 'test':
                invoker = new DummyInvoker("test invoker")
                break
            case 'local':
                def rundeckPort = (System.getenv("RD_PORT") ?: "4440")
                invoker = new RundeckInvoker("localhost", rundeckPort as int, appConfig)
                break
            default:
                invoker = new RundeckInvoker("rundeck.central.criteo.${env}", 443, appConfig)
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


class RundeckInvoker extends HttpInvoker {
    String host
    private String authToken

    RundeckInvoker(String host, int port, RootAppConfig appConfig) {
        super(host, port, 'rundeck')
        this.authToken = appConfig.under("tokens").get(name())
    }

    // how Rundeck is currently configured
    private int apiVersion = 16

    // Rundeck's API
    private String authenticated(String path) { "$path?authtoken=$authToken" }

    private String runPath(String jobName) {
        authenticated("/api/$apiVersion/job/$jobName/executions")
    }
    private def errorInHtml = /.+<p>(.+)<\/p>.+/

    // internal purpose
    private def jsonSlurper = new JsonSlurper()


    @Override
    Request buildRequest(long execTraceId, String executionKind, String productName, Version version, String target, String frozenParameters, String initiator) {
        def parameters = jsonSlurper.parseText(frozenParameters) as Map
        String jobName = parameters.jobName
        String uploader = parameters.uploaderVersion
        String quotedVersion = version.toString()
        if (quotedVersion.startsWith('['))
            quotedVersion = "'$quotedVersion'"

        def args = "-callback-url '${callbackUrl(execTraceId)}' -product-name '$productName' -target '$target' -product-version $quotedVersion"
        if (uploader)
            args += " -uploader-version " + uploader
        def body = [
                // before version 18 of Rundeck, we can't pass options in a structured way
                "argString": args
        ]
        body = new JsonBuilder(body).toString()
        def jsonType = Message$.MODULE$.ContentTypeJson

        Request req = Request.apply(Method.Post$.MODULE$, runPath(jobName))
        def headers = req.headerMap()
        headers[Fields.Host()] = host()
        headers[Fields.ContentType()] = jsonType
        headers[Fields.Accept()] = jsonType // default response format is XML
        req.write(body)
        req
    }

    @Override
    String extractLogHref(String executorAnswer) {
        (jsonSlurper.parseText(executorAnswer) as Map).permalink as String
    }

    @Override
    String extractMessage(int status, String content) {
        try {
            (jsonSlurper.parseText(content) as Map).message
        } catch (JsonException ignored) {
            def matcher = content =~ errorInHtml
            matcher.matches() ? matcher[0][1] : ""
        } catch (Throwable ignored) {
            ""
        }
    }
}
