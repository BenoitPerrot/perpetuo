package com.criteo.perpetuo

import com.criteo.perpetuo.config.ExternalData
import groovyx.net.http.HttpResponseException
import groovyx.net.http.RESTClient


class CriteoExternalData extends ExternalData {
    @Override
    LinkedHashMap<String, Object> forProduct(String productName) {
        def client = new RESTClient("http://moab.criteois.lan")
        try {
            def resp = client.get(path: "/products/$productName/manifest.json")
            assert resp.status == 200
            return resp.data
        } catch (HttpResponseException e) {
            assert e.response.status == 404 // it's allowed to not have data about a product
            return [:]
        }
    }
}
