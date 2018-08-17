package com.criteo.perpetuo.app

import java.util.logging.Logger

import com.criteo.perpetuo.config.AppConfigProvider
import io.prometheus.client.exporter.HTTPServer

class MetricsServer(val port: Int) {
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.info(s"Enabling metrics endpoint on port $port")
  val server: HTTPServer = new HTTPServer(port)
}
