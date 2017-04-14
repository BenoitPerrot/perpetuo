package com.criteo.perpetuo

import com.criteo.perpetuo.config.AppConfig$
import com.criteo.perpetuo.config.Hooks
import com.criteo.perpetuo.model.DeploymentRequest
import com.criteo.perpetuo.model.Target
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient

import static groovyx.net.http.ContentType.JSON


class CriteoHooks extends Hooks {
    @Override
    void onDeploymentRequestCreated(DeploymentRequest deploymentRequest, boolean immediateStart) {
        if (AppConfig$.MODULE$.env().endsWith("prod") && !immediateStart) {
            def productName = deploymentRequest.product().name()
            def version = deploymentRequest.version().toString()
            def target = Target.getSimpleSelectExpr(deploymentRequest.parsedTarget()).toUpperCase().replaceAll(",", ", ")
            def optComment = deploymentRequest.comment() ? "Initiator's comment: ${deploymentRequest.comment()}\n" : ""
            def desc = """
                Please deploy ${productName} to: ${target}
                Request initiated here: ${AppConfig$.MODULE$.get("selfUrl")}/deployment-requests/${deploymentRequest.id()}
                ${optComment}-- Perpetuo""".stripMargin()
            def suffix = "for $productName #${deploymentRequest.version()}"
            def body = [
                    "fields": [
                            "project"          : ["key": "MRM"],
                            "summary"          : "$productName (MESOS JMOAB #$version)".toString(),
                            "issuetype"        : ["name": "[MOAB] Release"],
                            "customfield_11006": ["value": "Mesos"],
                            "customfield_11003": version,
                            "customfield_12500": version,
                            "customfield_12702": ["value": "False"],
                            "customfield_12703": ["value": "Worldwide"],
                            "customfield_15500": ["value": "False"],
                            "customfield_27000": ["value": "Mesos"],
                            "customfield_11107": productName,
                            "reporter"         : ["name": deploymentRequest.creator()],
                            "description"      : desc
                    ]
            ]

            def url = AppConfig$.MODULE$.env() == "prod" ? "https://jira.criteois.com" : "https://dev2-jira.criteois.com"
            // don't reuse the connexion pool because:
            // - we are in a multi-threaded context managed externally
            // - we don't get here very often
            // - there can be a very long time between two calls
            def client = new RESTClient(url)
            // cannot use auth facility provided by RESTClient since it fires a pre-request
            // first without credentials and expects JIRA to respond a 401 but receives a 400...
            def credentials = AppConfig$.MODULE$.get("jira.user") + ":" + AppConfig$.MODULE$.get("jira.password")
            client.setHeaders(Authorization: "Basic ${credentials.bytes.encodeBase64()}")

            try {
                def resp = client.post(
                        path: "/rest/api/2/issue",
                        requestContentType: JSON,
                        body: body
                )
                assert resp.status < 300
                logger().info("Jira ticket created $suffix")
            } catch (HttpResponseException e) {
                logger().severe("Bad response from JIRA when creating ticket $suffix: ${e.response.status} ${e.getMessage()}: ${e.response.getData().toString()}")
            }
        }
    }
}
