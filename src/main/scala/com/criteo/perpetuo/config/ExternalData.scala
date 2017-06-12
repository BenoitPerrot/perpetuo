package com.criteo.perpetuo.config


trait BaseExternalData {

  def validateVersion(productName: String, version: String): java.util.List[String]

  def suggestVersions(productName: String): java.util.List[String]
}


class ExternalData extends BaseExternalData with Plugin {

  def validateVersion(productName: String, version: String): java.util.List[String] = new java.util.ArrayList[String]

  def suggestVersions(productName: String): java.util.List[String] = new java.util.ArrayList[String]

  val timeout_s = 5
}


private[config] class ExternalDataGetter(implementation: Option[ExternalData]) extends PluginRunner(implementation, new ExternalData) with BaseExternalData {

  def validateVersion(productName: String, version: String): java.util.List[String] =
    wrap(_.validateVersion(productName, version))

  def suggestVersions(productName: String): java.util.List[String] =
    wrap(_.suggestVersions(productName))
}
