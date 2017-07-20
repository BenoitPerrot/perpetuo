package com.criteo.perpetuo.config

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.engine.dispatchers.TargetDispatcher
import com.criteo.perpetuo.engine.executors.DummyInvoker
import com.criteo.perpetuo.engine.executors.ExecutorInvoker
import com.criteo.perpetuo.engine.executors.HttpInvoker
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
}


class RundeckInvoker extends HttpInvoker {
    String host
    private String authToken

    RundeckInvoker(String host, int port, RootAppConfig appConfig) {
        super(host, port, 'rundeck')
        this.authToken = appConfig.under("tokens").get(name())
    }

    // how Rundeck is currently configured
    private static String jobName(String productType, String executionKind) { "$executionKind-to-$productType" }
    private int apiVersion = 16

    // Rundeck's API
    private String authenticated(String path) { "$path?authtoken=$authToken" }

    private String runPath(String productType, String executionKind) {
        authenticated("/api/$apiVersion/job/${jobName(productType, executionKind)}/executions")
    }
    private def errorInHtml = /.+<p>(.+)<\/p>.+/

    // internal purpose
    private def jsonBuilder = new JsonOutput()
    private def jsonSlurper = new JsonSlurper()


    @Override
    String freezeParameters(String executionKind, String productName, String jsonVersion) {
        String productType = CriteoExternalData.fetchManifest(productName)?.get('type') ?: 'marathon' // fixme: only accept active products here (https://jira.criteois.com/browse/DREDD-309)

        jsonBuilder.toJson([
                runPath        : runPath(productType, executionKind),
                productName    : productName,
                productVersion : jsonSlurper.parseText(jsonVersion),
                // todo: read uploader version from version itself or infer the latest
                uploaderVersion: System.getenv("${productType.toUpperCase()}_UPLOADER")
        ])
    }

    @Override
    Request buildRequest(long executionId, String target, String frozenParameters, String initiator) {
        def parameters = jsonSlurper.parseText(frozenParameters) as Map
        def serializedVersion = parameters['productVersion'] instanceof String ? parameters['productVersion'] : jsonBuilder.toJson(parameters['productVersion'])
        def args = "-callback-url '${callbackUrl(executionId)}' -product-name '${parameters['productName']}' -target '$target' -product-version '$serializedVersion'"
        def uploader = parameters['uploaderVersion'] as String
        if (uploader)
            args += " -uploader-version " + uploader
        def body = [
                // before version 18 of Rundeck, we can't pass options in a structured way
                "argString": args
        ]
        body = new JsonBuilder(body).toString()
        def jsonType = Message$.MODULE$.ContentTypeJson

        Request req = Request.apply(Method.Post$.MODULE$, parameters['runPath'] as String)
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
