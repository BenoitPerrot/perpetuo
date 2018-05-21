package com.criteo.perpetuo.util.json

import spray.json.{JsArray, JsObject}

case class JsArrayScanner(a: JsArray, path: Seq[String] = Seq()) extends JsValueScanner {
  def map[A](f: JsObjectScanner => A): Vector[A] =
    a.elements.zipWithIndex.map {
      case (o: JsObject, i: Int) => f(JsObjectScanner(o, path :+ i.toString))
      case (unknown, i: Int) => reportWrongType(i.toString, "an object", s"$unknown (${unknown.getClass.getSimpleName})")
    }
}
