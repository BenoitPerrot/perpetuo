package com.criteo.perpetuo.config

import scala.collection.JavaConverters._
import scala.collection.mutable


trait BaseExternalData {
  def forProduct(productName: String): java.util.Map[String, Any]

  def forProducts(productNames: java.lang.Iterable[String]): java.lang.Iterable[java.util.Map[String, Any]]
}


class ExternalData extends BaseExternalData with Plugin {
  /**
    * Return JSON-serializable product metadata as a Map where the key
    * "name" can't be used (it would be overridden with the product name).
    */
  def forProduct(productName: String): java.util.Map[String, Any] = new java.util.LinkedHashMap()

  /**
    * Default implementation to retrieve metadata of all products at once.
    * Override it if there is better to do than a query for each product.
    */
  def forProducts(productNames: java.lang.Iterable[String]): java.lang.Iterable[java.util.Map[String, Any]] =
    productNames.asScala.map(forProduct).asJava
}


private[config] class ExternalDataGetter(implementation: Option[ExternalData]) extends PluginRunner(implementation, new ExternalData) with BaseExternalData {
  def forProduct(productName: String): java.util.Map[String, Any] =
    wrap("forProduct", productName)

  def forProducts(productNames: java.lang.Iterable[String]): java.lang.Iterable[java.util.Map[String, Any]] =
    wrap("forProducts", productNames)

  def forProducts(productNames: Iterable[String]): Iterable[mutable.Map[String, Any]] =
    forProducts(productNames.asJava).asScala.map(_.asScala)
}
