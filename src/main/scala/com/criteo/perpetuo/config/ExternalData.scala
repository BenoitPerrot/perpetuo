package com.criteo.perpetuo.config


trait BaseExternalData {
  def lastValidVersion(productName: String): String

  def validateVersion(productName: String, version: String): Boolean
}


class ExternalData extends BaseExternalData with Plugin {
  def lastValidVersion(productName: String): String = ""

  def validateVersion(productName: String, version: String): Boolean = false

  override val timeout_s = 5
}


private[config] class ExternalDataGetter(implementation: Option[ExternalData]) extends PluginRunner(implementation, new ExternalData) with BaseExternalData {
  def lastValidVersion(productName: String): String =
    wrap("lastValidVersion", productName)

  def validateVersion(productName: String, version: String): Boolean =
    wrap("validateVersion", productName, version)
}
