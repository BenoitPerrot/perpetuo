package com.criteo.perpetuo.config

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.model.DeploymentRequest
import com.criteo.perpetuo.model.Operation
import com.criteo.perpetuo.model.OperationTrace
import com.criteo.perpetuo.model.Target
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import org.apache.http.HttpEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import java.util.concurrent.TimeoutException

import static groovyx.net.http.ContentType.JSON


/* the "public" class to be loaded as the actual plugin must be the first statement after the imports */

class CriteoListener extends DefaultListenerPlugin {

    class JiraClient {
        String host
        String basicAuthorization

        JiraClient(String host, String username, String password) {
            this.host = host
            this.basicAuthorization = "Basic ${"${username}:${password}".bytes.encodeBase64()}"
        }

        private RESTClient makeAuthorizedClient() {
            // don't reuse the connection pool because:
            // - we are in a multi-threaded context managed externally
            // - we don't get here very often
            // - there can be a very long time between two calls
            //
            // cannot use auth facility provided by RESTClient since it fires a pre-request
            // first without credentials and expects JIRA to respond a 401 but receives a 400...
            def client = new RESTClient(host)
            client.setHeaders(Authorization: basicAuthorization)
            client
        }

        def createTicket(Map fields) {
            makeAuthorizedClient().post(
                    path: '/rest/api/2/issue',
                    requestContentType: JSON,
                    body: [fields: fields]
            )
        }

        def attachToTicket(String ticketKey, byte[] binaryBody, String attachmentName) {
            HttpPost http = new HttpPost()
            http.setURI("${this.host}/rest/api/2/issue/$ticketKey/attachments".toURI())
            http.setHeader('Accept', JSON.toString())
            http.setHeader('X-Atlassian-Token', 'nocheck')
            http.setHeader('Authorization', basicAuthorization)

            MultipartEntityBuilder builder = MultipartEntityBuilder.create()
            builder.addBinaryBody('file', binaryBody, ContentType.APPLICATION_OCTET_STREAM, attachmentName)
            HttpEntity multipart = builder.build()
            http.setEntity(multipart)

            CloseableHttpClient httpClient = HttpClients.createDefault()
            httpClient.execute(http)
        }

        List fetchTicketChildren(parentTicketKey) {
            def resp = makeAuthorizedClient().get(
                    path: '/rest/api/2/search',
                    query: [jql: "parent=$parentTicketKey"]
            )
            resp.data.issues
        }

        String fetchTicketStatusId(ticketKey) {
            def resp = makeAuthorizedClient().get(path: "/rest/api/2/issue/$ticketKey")
            assert resp.status == 200
            resp.data.fields.status.id
        }

        def transitionTicket(String ticketKey, Map<Integer, String> transitionsIdsAndNames) {
            def client = makeAuthorizedClient()
            transitionsIdsAndNames.each { transitionId, transitionName ->
                logger().info("Applying `$transitionName` (id=$transitionId) on $ticketKey")
                try {
                    client.post(
                            path: "/rest/api/2/issue/$ticketKey/transitions",
                            requestContentType: JSON,
                            body: [
                                    transition: [id: transitionId]
                            ]
                    )
                }
                catch (HttpResponseException e) {
                    logger().severe("Bad response from JIRA when transitioning ticket $ticketKey: ${e.response.status} ${e.message}: ${e.response.data.toString()}")
                    throw e
                }
            }
        }

        def updateTicketField(String ticketKey, String fieldId, Object value) {
            def resp = makeAuthorizedClient().put(
                    path: "/rest/api/2/issue/$ticketKey",
                    requestContentType: JSON,
                    body: [fields: [(fieldId): value]]
            )
            assert resp.status < 300
        }

        def addCommentToTicket(String ticketKey, String comment) {
            def resp = makeAuthorizedClient().post(
                    path: "/rest/api/2/issue/$ticketKey/comment",
                    requestContentType: JSON,
                    body: [body: comment]
            )
            assert resp.status < 300
        }
    }

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

    DbBinding dbBinding
    RootAppConfig appConfig
    JiraClient jiraClient

    CriteoListener(DbBinding dbBinding, RootAppConfig appConfig) {
        this.dbBinding = dbBinding
        this.appConfig = appConfig
        if (appConfig.env() != 'preprod') {
            def jiraUser = appConfig.tryGet('jira.user')
            def jiraPassword = appConfig.tryGet('jira.password')
            if (jiraUser.empty)
                logger().severe('`jira.user` must be set')
            else if (jiraPassword.empty)
                logger().severe('`jira.password` must be set')
            else {
                this.jiraClient = new JiraClient("https://${appConfig.env() == 'prod' ? '' : 'dev2-'}jira.criteois.com", jiraUser.get().toString(), jiraPassword.get().toString())
            }
        }
    }

    @Override
    String onDeploymentRequestCreated(DeploymentRequest deploymentRequest, boolean immediateStart) {
        if (jiraClient && !immediateStart) {
            def jsonSlurper = new JsonSlurper()
            def productName = deploymentRequest.product().name()

            def rawVersion = deploymentRequest.version().toString()
            def versionJson = jsonSlurper.parseText(rawVersion)
            def version = versionJson
            if (versionJson.class == [].class) {
                version = versionJson.max { v -> v.ratio }.value
            }
            assert version.class == "".class

            def lastValidatedVersion = ''
            def comment = deploymentRequest.comment()
            if (comment) {
                def m = comment =~ /Last validated version: (.*)$/
                if (0 < m.count) {
                    lastValidatedVersion = m[0][1]
                }
                comment = "Initiator's comment: ${comment}\n"
            }

            def target = Target.getSimpleSelectForGroovy(deploymentRequest.parsedTarget())
            def originator = appConfig.transition(productName) ?
                    "by Perpetuo" :
                    "here: ${appConfig.get('selfUrl')}/deployment-requests/${deploymentRequest.id()}"
            def desc = """
                Please deploy $productName to: ${target.join(', ').toUpperCase()}
                Request initiated $originator
                $comment-- Perpetuo""".stripMargin()

            def allMetadata = fetchReleaseMetadata()
            Map metadata = allMetadata[productName] ?:
                    allMetadata.values().find { it["service_name"] == productName }
            assert metadata

            def bools = [(true): "True", (false): "False"]

            String componentName = metadata["component_name"]

            def fields = [
                    "project"          : ["key": "MRM"],
                    "summary"          : "$productName (${componentName?.plus(" ") ?: ""}JMOAB #$version)".toString(),
                    "issuetype"        : ["name": "[MOAB] Release"],
                    "customfield_11006": ["value": componentName ?: productName],
                    "customfield_11003": rawVersion,
                    "customfield_12500": lastValidatedVersion,
                    "customfield_12702": ["value": bools[metadata.getOrDefault("preprodNeededField", false)]],
                    "customfield_12703": ["value": metadata.getOrDefault("deployType", "Worldwide")],
                    "customfield_10922": target.collect { String dc -> ["value": targetMap[dc]] },
                    "customfield_15500": ["value": bools[metadata.getOrDefault("prodApprovalNeeded", false)]],
                    "customfield_27000": ["value": metadata["technicalData"]],
                    "customfield_12800": metadata.getOrDefault("validation", []).collect { ["value": it] },
                    "customfield_11107": productName,
                    "reporter"         : ["name": deploymentRequest.creator()],
                    "description"      : desc
            ]

            def suffix = "for $productName #$version"
            try {
                def resp = jiraClient.createTicket(fields)

                String ticket = resp.data.key
                logger().info("Jira ticket created $suffix: $ticket")
                String ticketUrl = "${this.jiraClient.host}/browse/$ticket"

                try {
                    def changelog = CriteoExternalData.fetchChangeLog(productName, version, lastValidatedVersion)
                    if (changelog) {
                        def jsonChangelog = JsonOutput.toJson(changelog)
                        def uploadResponse = jiraClient.attachToTicket(ticket, jsonChangelog.bytes, 'changelog.json')
                        assert uploadResponse.statusLine.statusCode == 200: "file attachment failed:" + uploadResponse.statusLine.toString()
                        def rangeMessage = lastValidatedVersion ? "from #$lastValidatedVersion to" : "for"
                        logger().info("Changelog $rangeMessage #$version has been attached to JIRA ticket $ticket")
                    }
                }
                catch (Throwable e) {
                    logger().severe("Could not provide a changelog to the JIRA ticket $ticket: ${e.message}")
                }

                if (appConfig.transition(productName)) {
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
                logger().severe("Bad response from JIRA when creating ticket $suffix: ${e.response.status} ${e.message}: ${e.response.data.toString()}")
            }
        }
    }

    @Override
    void onDeploymentRequestStarted(DeploymentRequest deploymentRequest, int startedExecutions, int failedToStart, boolean atCreation) {
        if (jiraClient && !atCreation) {
            // fixme: !atCreation is for short-term transition only, while we support dep.req. creation without ticket
            String parentTicketKey = findJiraTicket(deploymentRequest)
            assert parentTicketKey
            jiraClient.transitionTicket(parentTicketKey, startingParentTransitions())
            def issues = jiraClient.fetchTicketChildren(parentTicketKey)
            assert issues.size() == 1
            String childKey = issues[0].key
            jiraClient.updateTicketField(childKey, "customfield_20300", 1)
            jiraClient.transitionTicket(childKey, startingChildrenTransitions())
        }
    }

    @Override
    void onDeploymentRequestRetried(DeploymentRequest deploymentRequest, int startedExecutions, int failedToStart) {
        if (jiraClient) {
            String ticketKey = restartDeploymentTicket(deploymentRequest)
            jiraClient.addCommentToTicket(ticketKey, "Retry launched from Perpetuo")
        }
    }

    @Override
    void onDeploymentRequestRolledBack(DeploymentRequest deploymentRequest, int startedExecutions, int failedToStart) {
        if (jiraClient) {
            String ticketKey = restartDeploymentTicket(deploymentRequest)
            jiraClient.addCommentToTicket(ticketKey, "Rollback launched from Perpetuo")
        }
    }

    @Override
    void onOperationClosed(OperationTrace operationTrace, DeploymentRequest deploymentRequest, boolean succeeded) {
        if (jiraClient) {
            def ticketKey = waitForDeploymentTicket(deploymentRequest, ["10396"]).first // aka "[RM] Deploying"
            def transitions = operationTrace.kind() == Operation.revert() ? cancelTransitions() : closingTransitions(succeeded)
            jiraClient.transitionTicket(ticketKey, transitions)
        }
    }

    static Map fetchReleaseMetadata() {
        def client = new HTTPBuilder()
        StringReader resp = client.get(uri: "http://review.criteois.lan/gitweb?p=release/release-management.git;a=blob_plain;f=src/python/releaseManagement/jiraMoab/tlaVsAppObject.json")
        return new JsonSlurper().parse(resp) as Map
    }

    static def startingParentTransitions() {
        [
                151: '[RM] DEFINITION OK - Deploy -> [RM] PROD DEPLOYMENT ONGOING'
        ]
    }

    static def startingChildrenTransitions() {
        [
                311: '[RM] AWAITING DEPLOYMENT - Launch deployer -> [RM] DEPLOYING',
        ]
    }

    static def cancelTransitions() {
        [
                371: '[RM] DEPLOYING - Partly deployed -> AWAITING DEPLOYMENT',
                121: 'AWAITING DEPLOYMENT - Cancel -> [RM] Deploy cancelled'
        ]
    }

    static def closingTransitions(Boolean succeeded) {
        succeeded ?
                [
                        371: '[RM] DEPLOYING - Partly deployed -> AWAITING DEPLOYMENT',
                        401: 'AWAITING DEPLOYMENT - Deployed -> [RM] DEPLOYED',
                        471: '[RM] DEPLOYED - Deploy [No validation] -> DONE'
                ] :
                [
                        351: '[RM] DEPLOYING - Deployment failed -> [RM] DEPLOYMENT FAILED'
                ]
    }

    static def resetTransitions(Boolean afterSuccess) {
        afterSuccess ?
                [
                        111: 'DONE - Reverse[Debug] -> [RM] DEPLOYED',
                        331: '[RM] DEPLOYED - Reverse[Debug] -> AWAITING DEPLOYMENT'
                ] :
                [
                        361: '[RM] DEPLOYMENT FAILED - Retry -> AWAITING DEPLOYMENT'
                ]
    }

    String findJiraTicket(DeploymentRequest deploymentRequest) {
        def ticketKey = null
        def comment = deploymentRequest.comment()
        if (comment) {
            def m = comment =~ /ticket: ${this.jiraClient.host}\/browse\/(.*)$/
            if (0 < m.count) {
                ticketKey = m[0][1]
            }
        }
        ticketKey
    }

    Tuple2<String, String> waitForDeploymentTicket(DeploymentRequest deploymentRequest, Iterable<String> expectedStatusId) {
        def parentTicketKey = null
        def issues = null
        def deadline = System.currentTimeMillis() + timeout_s() * 1000
        while (System.currentTimeMillis() < deadline) {
            parentTicketKey = parentTicketKey ?: findJiraTicket(deploymentRequest)
            if (parentTicketKey) {
                issues = issues ?: jiraClient.fetchTicketChildren(parentTicketKey)
                assert issues.size() <= 1
                if (issues.size() == 1) {
                    def ticketKey = issues[0].key
                    def statusId = jiraClient.fetchTicketStatusId(ticketKey)
                    if (expectedStatusId.contains(statusId))
                        return new Tuple(ticketKey, statusId)
                }
            }
            sleep(1000)
        }
        throw new TimeoutException("Could not get deployment ticket for deployment request #${deploymentRequest.id()} in time")
    }

    String restartDeploymentTicket(DeploymentRequest deploymentRequest) {
        def doneId = "10017" // aka "Done"
        def deploymentFailedId = "10398" // aka "[RM] Deployment failed"
        def (String ticketKey, String statusId) = waitForDeploymentTicket(deploymentRequest, [doneId, deploymentFailedId])
        jiraClient.transitionTicket(ticketKey, resetTransitions(statusId == doneId) + startingChildrenTransitions())
        return ticketKey
    }
}
