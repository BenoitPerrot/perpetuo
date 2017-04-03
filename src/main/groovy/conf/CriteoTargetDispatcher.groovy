import com.criteo.perpetuo.app.AppConfig$
import com.criteo.perpetuo.dispatchers.ExecutorsByPoset
import com.criteo.perpetuo.dispatchers.TargetDispatcherByPoset
import com.criteo.perpetuo.executors.DummyInvoker
import com.criteo.perpetuo.executors.ExecutorInvoker
import com.criteo.perpetuo.executors.HttpInvoker
import com.criteo.perpetuo.model.Version
import com.twitter.finagle.http.Fields
import com.twitter.finagle.http.Message$
import com.twitter.finagle.http.Method
import com.twitter.finagle.http.Request
import groovy.json.JsonBuilder
import groovy.json.JsonException
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import scala.None$
import scala.Option
import scala.Some


/* first, the class exposed to actually configure Perpetuo */

class CriteoTargetDispatcher extends TargetDispatcherByPoset {
    CriteoTargetDispatcher() {
        super(getExecutors(AppConfig$.MODULE$.env))
    }

    CriteoTargetDispatcher(String env) {
        super(getExecutors(env))
    }

    private static ExecutorsByPoset getExecutors(String env) {
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
                        '*': new RundeckInvoker("localhost", rundeckPort as int, "preprod")
                ]
                break
            default:
                executorMap = [
                        '*': new RundeckInvoker("rundeck.central.criteo.${env}", 443, env)
                ]
        }

        ExecutorsByPoset.apply(executorMap, { _ -> ['*'] })
    }
}


class RundeckInvoker extends HttpInvoker {
    String host
    String marathonEnv

    RundeckInvoker(String host, int port, String marathonEnv) {
        super(host, port, 'rundeck')
        this.marathonEnv = marathonEnv
    }

    // how Rundeck is currently configured
    private static String jobName(String operationName) { "deploy-to-marathon" }
    private int apiVersion = 16

    // authentication
    private String authToken = AppConfig$.MODULE$.under("tokens").get(name())

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
    Request buildRequest(String operationName, long executionId, String productName, Version version, String rawTarget, String initiator) {
        def escapedTarget = jsonBuilder.toJson(rawTarget)
        def body = [
                // before version 18 of Rundeck, we can't pass options in a structured way
                "argString": // todo: remove 'environment'?
                        "-environment $marathonEnv -callback-url '${callbackUrl(executionId)}' -product-name '$productName' -product-version '$version' -target $escapedTarget"
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
    Option<String> tryExtractMessage(int status, String content) {
        try {
            new Some((jsonSlurper.parseText(content) as Map).message as String)
        } catch (JsonException ignored) {
            def matcher = content =~ errorInHtml
            if (matcher.matches()) new Some(matcher[0][1]) else None$.MODULE$
        } catch (Throwable ignored) {
            None$.MODULE$
        }
    }
}
