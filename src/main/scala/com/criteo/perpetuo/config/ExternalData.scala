package com.criteo.perpetuo.config


trait BaseExternalData {
  def lastValidVersion(productName: String): String

  def validateVersion(productName: String, version: String): String
}


class ExternalData extends BaseExternalData with Plugin {
  def lastValidVersion(productName: String): String = ""

  def validateVersion(productName: String, version: String): String = """{"valid": false, "reason": "default"}"""

  val timeout_s = 5
}


private[config] class ExternalDataGetter(implementation: Option[ExternalData]) extends PluginRunner(implementation, new ExternalData) with BaseExternalData {
  def lastValidVersion(productName: String): String =
    wrap(_.lastValidVersion(productName))

  def validateVersion(productName: String, version: String): String =
    wrap(_.validateVersion(productName, version))
}
