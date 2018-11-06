package com.criteo.perpetuo.metrics

import com.samstarling.prometheusfinagle.metrics.Telemetry
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Future
import io.prometheus.client.Counter

class HttpMonitoringFilter(telemetry: Telemetry,
                           labeler: HttpServiceLabeller = new HttpServiceLabeller)
  extends SimpleFilter[Request, Response] {

  private val requests: Counter = telemetry.counter(
    name = "incoming_http_requests_total",
    help = "The number of incoming HTTP requests",
    labelNames = labeler.keys
  )

  override def apply(request: Request,
                     service: Service[Request, Response]): Future[Response] = {
    service(request).map(response => {
      if (request.uri.startsWith("/api")) { // filter-out static content
        requests.labels(labeler.labelsFor(request, response): _*).inc()
      }
      response
    })
  }
}