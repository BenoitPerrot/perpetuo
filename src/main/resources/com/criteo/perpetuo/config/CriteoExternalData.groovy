package com.criteo.perpetuo.config

import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient

/* the "public" class to be loaded as the actual plugin must be the first statement after the imports */

class CriteoExternalData extends ExternalData { // fixme: this only works with JMOAB for now

    static final blacklisted = "Blacklisted"

    @Override
    java.util.List<String> validateVersion(String productName, String version) {
        Map<String, ?> manifest = fetchManifest(productName)
        Map<String, String> allRepos = fetchArtifactToRepository(version)
        Map<String, ?> allValidation = fetchIsValid(version)
        if (manifest == null) {
            ["Product does not exist or has never been built."]
        } else if (manifest.artifacts == null) {
            [] // cannot do validation for that product...
        } else if (allRepos == null || allValidation == null) {
            ["No data could be found. Version may be still being built or packaged."]
        } else {
            def repos = manifest.artifacts.collect {
                allRepos.get("${it.groupId}:${it.artifactId}".toString())
            }
            def errors = []
            for (String repo : repos) {
                def validation = allValidation.get(repo)
                if (!validation) {
                    errors += "Could not find artifact for ${repo}".toString()
                } else if (!validation.valid) {
                    String reason = validation.reason
                    if (reason == blacklisted && validation.comment) {
                        reason = "${reason} because: ${validation.comment}"
                    }
                    errors += "${repo}: ${reason}".toString()
                }
            }
            errors
        }
    }

    static Map<String, ?> getChangeLog(String productName, String version, String previousVersion) {
        Map<String, ?> manifest = fetchManifest(productName)
        Map<String, String> allRepos = fetchArtifactToRepository(version)
        if (manifest?.artifacts == null || allRepos == null) {
            [:]
        } else {
            def repos = manifest.artifacts.collect {
                allRepos.get(it.groupId + ':' + it.artifactId)
            }

            def listOfChanges = [:]
            for (String repo : repos) {
                def changelog = getChangeLogForRepo(repo, version, previousVersion)
                for (def moab : changelog) {
                    for (def commit : moab.commits) {
                        if (!listOfChanges[moab.moabId])
                            listOfChanges[moab.moabId] = [:]
                        if (!listOfChanges[moab.moabId][commit.repo])
                            listOfChanges[moab.moabId][commit.repo] = [:]
                        if (!listOfChanges[moab.moabId][commit.repo][commit.sha1])
                            listOfChanges[moab.moabId][commit.repo][commit.sha1] = [
                                author:commit.author,
                                date:commit.date,
                                title:commit.title,
                                message:commit.message
                            ]
                    }
                }
            }
            listOfChanges
        }
    }

    @Override
    java.util.List<String> suggestVersions(String productName) {
        Map<String, ?> productManifest = fetchManifest(productName)

        if (productManifest?.artifacts == null)
            return []

        def requiredArtifactToLatestVersions = productManifest.artifacts.collectEntries { artifact ->
            String groupId = artifact.groupId
            String artifactId = artifact.artifactId
            [(artifact): fetchLatestVersions(groupId, artifactId)]
        }

        return requiredArtifactToLatestVersions.inject(null as List<String>) { suggestions, requiredArtifact, latestVersions ->
            suggestions == null ? latestVersions : suggestions.findAll { latestVersions.contains(it) }
        } ?: []
    }

    static Map<String, ?> fetchManifest(String productName) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/products/$productName/manifest.json")
            assert resp.status == 200
            return resp.data
        } catch (HttpResponseException e) {
            assert e.response.status == 404 // it's not an error to not have data (yet?) about a product
            return null
        }
    }

    static Map<String, String> fetchArtifactToRepository(String requestedVersion) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/java/moabs/$requestedVersion/dependency-graph.json")
            assert resp.status == 200
            return resp.data.collectMany { repo ->
                repo.getOrDefault('artifacts', [:]).getOrDefault('created', []).collect {
                    [it, repo.get('name')]
                }
            }.collectEntries { it }
        } catch (HttpResponseException e) {
            assert e.response.status == 404 // it's allowed to not have data about this MOAB
            return null
        }
    }

    static Map<String, ?> fetchIsValid(String requestedVersion) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/java/moabs/$requestedVersion/product-validation.json")
            assert resp.status == 200

            return resp.data
        } catch (HttpResponseException e) {
            assert e.response.status == 404 // it's allowed to not have data about this MOAB
            return null
        }
    }

    static List<String> fetchLatestVersions(String groupId, String artifactId) {
        def client = new RESTClient("http://moab.criteois.lan")

        def resp = client.get(path: "/tags/build/${groupId}/${artifactId}/latest.json")
        assert resp.status == 200

        return resp.data.collect { it['version'] }
    }

    static List<Map<String, ?>> getChangeLogForRepo(String repoName, String requestedVersion, String previousVersion) {
        def client = new RESTClient("http://software-factory-services.marathon-par.central.criteo.preprod")
        def query = [
                'repositories': repoName,
                'with-dependencies': true,
                'format': 'json',
        ]
        if (previousVersion) {  // If previous version is not given, we might want to display the changes for the current version
            query += ['since': previousVersion]
        }
        try {
            def resp = client.get(
                    path: "/api/jmoabs/$requestedVersion/log",
                    query: query
            )
            assert resp.status == 200
            return resp.data
        } catch (HttpResponseException e) {
            assert e.response.status == 404
            return []
        }
    }
}
