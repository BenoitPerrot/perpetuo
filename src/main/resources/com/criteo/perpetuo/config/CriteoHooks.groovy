package com.criteo.perpetuo.config

import com.criteo.perpetuo.dao.DbBinding
import com.criteo.perpetuo.model.DeploymentRequest
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

import static groovyx.net.http.ContentType.JSON


/* the "public" class to be loaded as the actual plugin must be the first statement after the imports */

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

    DbBinding dbBinding
    RootAppConfig appConfig

    CriteoHooks(DbBinding dbBinding, RootAppConfig appConfig) {
        this.dbBinding = dbBinding
        this.appConfig = appConfig
    }

    @Override
    String onDeploymentRequestCreated(DeploymentRequest deploymentRequest, boolean immediateStart) {
        if (appConfig.env() != "preprod" && !immediateStart) {
            def jsonSlurper = new JsonSlurper()
            def productName = deploymentRequest.product().name()

            def rawVersion = deploymentRequest.version().toString()
            def version
            def versionJson = jsonSlurper.parseText(rawVersion)
            version = versionJson
            if (versionJson.getClass() == [].getClass()) {
                version = versionJson.max { v -> v.ratio }.value
            }
            assert version.getClass() == "".getClass()

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
            def originator = appConfig.transition() ?
                    "by Perpetuo" :
                    "here: ${appConfig.get('selfUrl')}/deployment-requests/${deploymentRequest.id()}"
            def desc = """
                Please deploy $productName to: ${target.join(', ').toUpperCase()}
                Request initiated $originator
                $comment-- Perpetuo""".stripMargin()

            def allMetadata = getReleaseMetadata()
            Map metadata = allMetadata[productName] ?:
                    allMetadata.values().find { it["service_name"] == productName }
            assert metadata

            def bools = [(true): "True", (false): "False"]

            String componentName = metadata["component_name"]

            def body = [
                    "fields": [
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

            def suffix = "for $productName #$version"
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

                def changelog = CriteoExternalData.getChangeLog(productName, version, lastValidatedVersion)
                if (changelog) {
                    def jsonChangelog = JsonOutput.toJson(changelog)
                    def attachmentUri = url + "/rest/api/2/issue/$ticket/attachments"

                    HttpPost http = new HttpPost()
                    http.setURI(attachmentUri.toURI())
                    http.setHeader('Accept', JSON.toString())
                    http.setHeader('X-Atlassian-Token' , 'nocheck')
                    http.setHeader('Authorization', "Basic ${credentials.bytes.encodeBase64()}")

                    MultipartEntityBuilder builder = MultipartEntityBuilder.create()
                    builder.addBinaryBody("file", jsonChangelog.bytes, ContentType.APPLICATION_OCTET_STREAM, "changelog.json")
                    HttpEntity multipart = builder.build()
                    http.setEntity(multipart)

                    CloseableHttpClient httpClient = HttpClients.createDefault()
                    def uploadResponse = httpClient.execute(http)

                    if (uploadResponse.statusLine.statusCode == 200) {
                        def rangeMessage = lastValidatedVersion ? "from #$lastValidatedVersion to" : "for"
                        logger().info("Changelog $rangeMessage #$version has been attached to JIRA Issue $ticket")
                    } else {
                        logger().warning('Could not attach changelog from file to JIRA Issue:' + uploadResponse.statusLine.toString())
                    }
                }


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

    static Map getReleaseMetadata() {
        def client = new HTTPBuilder()
        StringReader resp = client.get(uri: "http://review.criteois.lan/gitweb?p=release/release-management.git;a=blob_plain;f=src/python/releaseManagement/jiraMoab/tlaVsAppObject.json")
        return new JsonSlurper().parse(resp) as Map
    }
}
