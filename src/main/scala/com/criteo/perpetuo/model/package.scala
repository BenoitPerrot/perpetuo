package com.criteo.perpetuo

import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}

import scala.collection.JavaConverters._


package object model {

  type TargetExpr = Set[String]

  implicit class TargetGroups[A, B](javaTargetGroups: JavaMap[A, JavaIterable[B]]) {
    def iterateAsScala: Iterator[(A, Set[B])] =
      javaTargetGroups.entrySet.iterator.asScala
        .map(entry => entry.getKey -> entry.getValue.iterator.asScala.toSet)
  }

}
