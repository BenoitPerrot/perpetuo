package com.criteo.perpetuo.config

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.dispatchers.ExecutorsByPoset
import com.criteo.perpetuo.dispatchers.TargetDispatcherByPoset
import com.criteo.perpetuo.executors.DummyInvoker
import com.criteo.perpetuo.executors.ExecutorInvoker
import com.criteo.perpetuo.executors.HttpInvoker
import com.twitter.finagle.http.Fields
import com.twitter.finagle.http.Message$
import com.twitter.finagle.http.Method
import com.twitter.finagle.http.Request
import groovy.json.JsonBuilder
import groovy.json.JsonException
import groovy.json.JsonOutput
import groovy.json.JsonSlurper


/* the "public" class to be loaded as the actual plugin must be the first statement after the imports */

class CriteoTargetDispatcher extends TargetDispatcherByPoset {
    CriteoTargetDispatcher(DbBinding dbBinding, RootAppConfig appConfig) {
        super(getExecutors(appConfig))
    }

    private static ExecutorsByPoset getExecutors(RootAppConfig appConfig) {
        def env = appConfig.env()
        Map<String, ExecutorInvoker> executorMap
        switch (env) {
            case 'test':
                executorMap = [
                        '*': new DummyInvoker("test invoker")
                ]
                break
            case 'local':
                def rundeckPort = (System.getenv("RD_PORT") ?: "4440")
                executorMap = [
                        '*': new RundeckInvoker("localhost", rundeckPort as int, "preprod", appConfig)
                ]
                break
            default:
                executorMap = [
                        '*': new RundeckInvoker("rundeck.central.criteo.${env}", 443, env, appConfig)
                ]
        }

        ExecutorsByPoset.apply(executorMap, { _ -> ['*'] })
    }
}


class RundeckInvoker extends HttpInvoker {
    String host
    String marathonEnv
    private String authToken

    RundeckInvoker(String host, int port, String marathonEnv, RootAppConfig appConfig) {
        super(host, port, 'rundeck')
        this.marathonEnv = marathonEnv
        this.authToken = appConfig.under("tokens").get(name())
    }

    // how Rundeck is currently configured
    private static String jobName(String operationName) { "deploy-to-marathon" }
    private int apiVersion = 16

    // Rundeck's API
    private String authenticated(String path) { "$path?authtoken=$authToken" }

    private String runPath(String operationName) {
        authenticated("/api/$apiVersion/job/${jobName(operationName)}/executions")
    }
    private def errorInHtml = /.+<p>(.+)<\/p>.+/

    // internal purpose
    private def jsonBuilder = new JsonOutput()
    private def jsonSlurper = new JsonSlurper()


    @Override
    Request buildRequest(String operationName, long executionId, String productName, String version, String target, String initiator) {
        def escapedProductName = jsonBuilder.toJson(productName)
        def escapedVersion = jsonBuilder.toJson(version)
        def escapedTarget = jsonBuilder.toJson(target)
        def args = "-environment $marathonEnv -callback-url '${callbackUrl(executionId)}' -product-name $escapedProductName -product-version $escapedVersion -target $escapedTarget"
        def uploader = System.getenv("MARATHON_UPLOADER")
        if (uploader)
            args += " -uploader-version " + uploader
        def body = [
                // before version 18 of Rundeck, we can't pass options in a structured way
                "argString": args // todo: remove 'environment'?
        ]
        body = new JsonBuilder(body).toString()
        def jsonType = Message$.MODULE$.ContentTypeJson

        Request req = Request.apply(Method.Post$.MODULE$, runPath(operationName))
        def headers = req.headerMap()
        headers[Fields.Host()] = host()
        headers[Fields.ContentType()] = jsonType
        headers[Fields.Accept()] = jsonType // default response format is XML
        req.write(body)
        req
    }

    @Override
    String getLogHref(String executorAnswer) {
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
