package com.criteo.perpetuo.metrics

import com.samstarling.prometheusfinagle.metrics.Telemetry
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Future, Stopwatch}
import io.prometheus.client.{Counter, Histogram}

class HttpMonitoringFilter(telemetry: Telemetry,
                           buckets: Seq[Double] = Seq(0.001, 0.01, 0.1, 1, 10),
                           labeler: HttpServiceLabeller = new HttpServiceLabeller)
  extends SimpleFilter[Request, Response] {

  private val requests: Counter = telemetry.counter(
    name = "incoming_http_requests_total",
    help = "The number of incoming HTTP requests",
    labelNames = labeler.keys
  )

  private val histogram: Histogram = telemetry.histogram(
    name = "incoming_http_request_latency_seconds",
    help = "A histogram of the response latency for HTTP requests",
    labelNames = labeler.keys,
    buckets = buckets
  )

  override def apply(request: Request,
                     service: Service[Request, Response]): Future[Response] = {
    val stopwatch = Stopwatch.start()
      service(request).map(response => {
      if (request.uri.startsWith("/api")) { // filter-out static content
        requests.labels(labeler.labelsFor(request, response): _*).inc()

        histogram
          .labels(labeler.labelsFor(request, response): _*)
          .observe(stopwatch().inMilliseconds / 1000.0)
      }
      response
    })
  }
}