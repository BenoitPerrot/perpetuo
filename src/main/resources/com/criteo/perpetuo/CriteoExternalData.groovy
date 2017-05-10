import com.criteo.perpetuo.config.ExternalData
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient
import org.codehaus.groovy.runtime.metaclass.ConcurrentReaderHashMap


/* first, the class exposed to actually configure Perpetuo */

class CriteoExternalData extends ExternalData { // fixme: this only works with JMOAB for now
    final static int maxVersionsInThePast = 200 // don't bother reading further in the past

    final artifacts = new Artifacts()
    final repos = new Repos()
    final is_packaged = new IsPackaged()

    @Override
    boolean validateVersion(String productName, String version) {
        Map<String, String> allRepos = repos.get(version)
        Set<String> allPackaged = is_packaged.get(version)
        def art = artifacts.get(productName)
        return art != null && allRepos != null && allPackaged != null && art.collect {
            allRepos.get(it)
        }.every { it in allPackaged }
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


class Artifacts extends Cache<List<String>> {
    @Override
    List<String> fetch(String productName) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/products/$productName/manifest.json")
            assert resp.status == 200
            return resp.data.get('artifacts').collect {
                it.get('groupId') + ':' + it.get('artifactId')
            }
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
