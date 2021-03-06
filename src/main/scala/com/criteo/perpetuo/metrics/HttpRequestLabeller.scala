package com.criteo.perpetuo.metrics

import com.twitter.finagle.http.{Request, Response}

trait ServiceLabeller[Req, Rep] {
  def keys: Seq[String]
  def labelsFor(request: Req, response: Rep): Seq[String]
}

class HttpServiceLabeller extends ServiceLabeller[Request, Response] {
  def keys: Seq[String] = Seq("uri", "status", "statusClass", "method")

  def labelsFor(request: Request, response: Response): List[String] = List(
    request.uri.replaceAll("/[0-9]+", "/:id"), // replace ids with marker
    response.status.code.toString,
    s"${response.status.code.toString.charAt(0)}xx",
    request.method.toString
  )
}