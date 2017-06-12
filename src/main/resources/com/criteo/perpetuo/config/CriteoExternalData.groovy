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

    @Override
    java.util.List<String> suggestVersions(String productName) {
        def l = []
        try {
            Map<String, ?> productManifest = fetchManifest(productName)

            def requiredArtifactToLatestVersions = productManifest.get('artifacts').collectEntries { artifact ->
                String groupId = artifact.get('groupId')
                String artifactId = artifact.get('artifactId')
                [ (artifact): fetchLatestVersions(groupId, artifactId) ]
            }

            l = requiredArtifactToLatestVersions.inject(null as List<String>) { suggestions, requiredArtifact, latestVersions ->
                suggestions == null ? latestVersions : suggestions.findAll { latestVersions.contains(it) }
            }

        } catch (Exception e) {
            System.err.println(e.getStackTrace())
        }
        return l
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

    static Map<String, String> fetchArtifactToRepository(String version) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/java/moabs/$version/dependency-graph.json")
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

    static Map<String, ?> fetchIsValid(String version) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/java/moabs/$version/product-validation.json")
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
}
