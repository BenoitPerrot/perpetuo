package com.criteo.perpetuo.metrics

import com.samstarling.prometheusfinagle.metrics.MetricsService
import com.twitter.finagle.http.Request
import com.twitter.finatra.http.{Controller => BaseController}
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.hotspot.DefaultExports
import javax.inject.Singleton

@Singleton
class Controller
  extends BaseController {
  DefaultExports.initialize()
  val registry: CollectorRegistry = CollectorRegistry.defaultRegistry
  val metricsService: MetricsService = new MetricsService(registry)
  get("/metrics") { r: Request =>
    metricsService(r)
      .map { resp =>
        resp.setContentType("text/plain")
        resp
      }
  }
}
