package com.criteo.perpetuo

import scala.io.Source


object TestUtils {
  def getResourceAsString(resourceFileName: String): String = {
    val cls = Class.forName(Thread.currentThread.getStackTrace.apply(2).getClassName)
    Source.fromURL(cls.getResource(resourceFileName)).mkString
  }
}
