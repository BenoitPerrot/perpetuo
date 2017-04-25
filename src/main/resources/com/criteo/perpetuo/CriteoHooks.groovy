package com.criteo.perpetuo

import com.criteo.perpetuo.config.Hooks
import com.criteo.perpetuo.config.RootAppConfig
import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.model.DeploymentRequest
import com.criteo.perpetuo.model.Target
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import static groovyx.net.http.ContentType.JSON


class CriteoHooks extends Hooks {
    def targetMap = [
            "sv6": "NA-SV6",
            "ny8": "NA-NY8",
            "va1": "NA-VA1",
            "par": "EU-PAR",
            "am5": "EU-AM5",
            "hk5": "AS-HK5",
            "sh5": "CN-SH5",
            "ty5": "AS-TY5",
            "*"  : "Worldwide"
    ]
    def dcByDcApps = [
            "bidding-direct-bidder-app",
            "bidding-direct-bidder-canary-app",
            "imageproxy-app",
            "videoproxy-app"
    ]

    DbBinding dbBinding
    RootAppConfig appConfig

    CriteoHooks(DbBinding dbBinding, RootAppConfig appConfig) {
        this.dbBinding = dbBinding
        this.appConfig = appConfig
    }

    @Override
    String onDeploymentRequestCreated(DeploymentRequest deploymentRequest, boolean immediateStart) {
        if (appConfig.env().endsWith("prod") && !immediateStart) {
            def productName = deploymentRequest.product().name()
            def version = deploymentRequest.version().toString()
            def target = Target.getSimpleSelectForGroovy(deploymentRequest.parsedTarget())
            def optComment = deploymentRequest.comment() ? "Initiator's comment: ${deploymentRequest.comment()}\n" : ""
            def originator = appConfig.transition() ?
                    "by Perpetuo" :
                    "here: ${appConfig.get('selfUrl')}/deployment-requests/${deploymentRequest.id()}"
            def desc = """
                Please deploy ${productName} to: ${target.join(', ').toUpperCase()}
                Request initiated $originator
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
                            "customfield_12703": ["value": dcByDcApps.contains(productName) ? "DC by DC" : "Worldwide"],
                            "customfield_10922": target.collect { String dc -> ["value": targetMap[dc]] },
                            "customfield_15500": ["value": "False"],
                            "customfield_27000": ["value": "Mesos"],
                            "customfield_11107": productName,
                            "reporter"         : ["name": deploymentRequest.creator()],
                            "description"      : desc
                    ]
            ]

            def url = appConfig.env() == "prod" ? "https://jira.criteois.com" : "https://dev2-jira.criteois.com"
            // don't reuse the connexion pool because:
            // - we are in a multi-threaded context managed externally
            // - we don't get here very often
            // - there can be a very long time between two calls
            def client = new RESTClient(url)
            // cannot use auth facility provided by RESTClient since it fires a pre-request
            // first without credentials and expects JIRA to respond a 401 but receives a 400...
            def credentials = appConfig.get("jira.user") + ":" + appConfig.get("jira.password")
            client.setHeaders(Authorization: "Basic ${credentials.bytes.encodeBase64()}")

            try {
                def resp = client.post(
                        path: "/rest/api/2/issue",
                        requestContentType: JSON,
                        body: body
                )
                assert resp.status < 300

                String ticket = resp.data.key
                logger().info("Jira ticket created $suffix: $ticket")
                String ticketUrl = "$url/browse/$ticket"

                if (appConfig.transition()) {
                    return ticketUrl
                } else {
                    def newComment = deploymentRequest.comment()
                    if (newComment)
                        newComment += "\n"
                    newComment += "ticket: $ticketUrl"
                    def update = dbBinding.updateDeploymentRequestComment(deploymentRequest.id(), newComment)
                    Boolean updated = Await.result(update, Duration.apply(5, "s"))
                    assert updated
                }
            } catch (HttpResponseException e) {
                logger().severe("Bad response from JIRA when creating ticket $suffix: ${e.response.status} ${e.getMessage()}: ${e.response.getData().toString()}")
            }
        }
    }
}
