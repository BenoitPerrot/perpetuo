package com.criteo.perpetuo.config

import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import org.codehaus.groovy.runtime.metaclass.ConcurrentReaderHashMap


/* the "public" class to be loaded as the actual plugin must be the first statement after the imports */

class CriteoExternalData extends ExternalData { // fixme: this only works with JMOAB for now
    final static int maxVersionsInThePast = 200 // don't bother reading further in the past

    final manifest = new Manifest()
    final repos = new Repos()
    final is_packaged = new IsPackaged()

    @Override
    String lastValidVersion(String productName) {
        def version = getLastVersion()
        if (!version)
            return ""

        // if we have a MOAB number (there can't be a patch number on this one) and if it's not valid,
        // let's find an older version that is valid
        def deadline = System.currentTimeMillis() + timeout_s() * 1000 * 0.8 // consume max. 80% of the general timeout
        def oldest = Math.max(0, version - maxVersionsInThePast)
        while (version > oldest && System.currentTimeMillis() < deadline) {
            if (validateVersion(productName, version as String))
                return version as String
            --version
        }
        return ""
    }

    @Override
    boolean validateVersion(String productName, String version) {
        Map<String, ?> manifest = manifest.get(productName)
        Map<String, String> allRepos = repos.get(version)
        Set<String> allPackaged = is_packaged.get(version)
        return manifest != null && allRepos != null && allPackaged != null &&
                manifest.get('artifacts').collect {
                    allRepos.get(it.get('groupId') + ':' + it.get('artifactId'))
                }.every { it in allPackaged }
    }

    static Integer getLastVersion() {
        // fixme: not acceptable in long term: it should take the product name as parameter
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/java/moabs/current/id")
            assert resp.status == 200
            return resp.data.text.trim() as Integer // it can't be a patched MOAB
        } catch (HttpResponseException e) {
            assert e.response.status == 404 // it's allowed to not have data about a product
            return null
        }
    }
}


abstract class Cache<T> extends ConcurrentReaderHashMap {
    final private Map<String, Long> expirationTimesMillis = [:] // expiration time for each key
    final private fetchLock = new Object()

    T get(String key) {
        def res
        if (System.currentTimeMillis() < expirationTimesMillis.get(key, 0)) {
            res = super.get(key) as T
        } else {
            synchronized (fetchLock) {
                // prevent unnecessary concurrent fetches of new data
                if (System.currentTimeMillis() < expirationTimesMillis.get(key, 0)) {
                    // if the lock was blocking, we're supposed to get here (if it was expired, expiration changed)
                    res = super.get(key) as T
                } else {
                    res = fetch(key)
                    if (res != null)
                        put(key, res)

                    def cacheValidity = res != null ? 600 : 10 // in seconds
                    expirationTimesMillis[key] = System.currentTimeMillis() + cacheValidity * 1000
                }
            }
        }
        return res
    }

    abstract T fetch(String key)
}


class Manifest extends Cache<Map<String, ?>> {
    @Override
    Map<String, ?> fetch(String productName) {
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
}


class Repos extends Cache<Map<String, String>> { // repos per artifact
    @Override
    Map<String, String> fetch(String version) {
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
}


class IsPackaged extends Cache<Set<String>> { // tells for each repo if it's successfully packaged
    @Override
    Set<String> fetch(String version) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/java/moabs/$version/green-projects.txt")
            assert resp.status == 200
            return resp.data.text.split(',') as Set<String>
        } catch (HttpResponseException e) {
            assert e.response.status == 404 // it's allowed to not have data about this MOAB
            return null
        }
    }
}
