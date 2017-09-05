package com.criteo.perpetuo.app

import javax.inject.Inject

import com.criteo.perpetuo.config.Plugins
import com.twitter.finatra.http.Controller

import scala.collection.JavaConverters._

class ExternalDataController @Inject()(val plugins: Plugins)
  extends Controller with TimeoutToHttpStatusTranslation {

  def suggestVersions(productName: String): Seq[String] =
    plugins.externalData.suggestVersions(productName).asScala

  def validateVersion(productName: String, productVersion: String): Seq[String] =
    plugins.externalData.validateVersion(productName, productVersion).asScala

  post("/api/products/suggest-versions") { r: ProductPost =>
    handleTimeout(
      suggestVersions(r.name)
    )
  }

  post("/api/products/validate-version") { r: ProductPostWithVersion =>
    handleTimeout {
      val reasonsForInvalidity = validateVersion(r.name, r.version)
      if (reasonsForInvalidity.isEmpty)
        Map("valid" -> true)
      else
        Map("valid" -> false, "reason" -> reasonsForInvalidity)
    }
  }
}
