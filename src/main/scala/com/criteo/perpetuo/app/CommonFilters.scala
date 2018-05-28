package com.criteo.perpetuo.app

import com.twitter.finagle.filter.LogFormatter
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finatra.filters.MergedFilter
import com.twitter.finatra.http.filters.{ExceptionMappingFilter, HttpResponseFilter, StatsFilter}
import com.twitter.inject.Logging
import com.twitter.util.{Future, Stopwatch}
import javax.inject.{Inject, Singleton}
import net.logstash.logback.marker.Markers
import collection.JavaConverters._


@Singleton
class CommonFilters @Inject()(a: StatsFilter[Request],
                              b: AccessLoggingFilter[Request],
                              c: HttpResponseFilter[Request],
                              d: ExceptionMappingFilter[Request])
  extends MergedFilter(a, b, c, d)


@Singleton
class AccessLoggingFilter[R <: Request] @Inject()(logFormatter: LogFormatter[R, Response])
  extends SimpleFilter[R, Response]
    with Logging {

  override def apply(request: R, service: Service[R, Response]): Future[Response] = {
    if (!isInfoEnabled) {
      service(request)
    }
    else {
      val elapsed = Stopwatch.start()
      service(request) onSuccess { response =>
        val elapsed_time = elapsed()
        val map = Map(
          "method" -> request.method.toString,
          "remote_port" -> request.remotePort,
          "remote_ip" -> request.remoteAddress,
          "remote_host" -> request.remoteHost.toString,
          "x_forwarded_for" -> response.xForwardedFor.getOrElse(""),
          "user_agent" -> request.userAgent.getOrElse(""),
          "response_content_type" -> response.contentType.getOrElse(""),
          "response_size_b" -> response.length,
          "request_size_b" -> request.length,
          "request_content_length_b" -> request.contentLength.getOrElse("unknown"),
          "request_content_length_b" -> response.contentLength.getOrElse("unknown"),
          "protocol" -> request.version.toString,
          "requested_uri" -> request.uri,
          "referer" -> response.referer.getOrElse(""),
          "response_status" -> response.statusCode,
          "response_time_ms" -> elapsed_time.inMilliseconds / 1000.0
        ).asJava
        logger.info(Markers.appendEntries(map), logFormatter.format(request, response, elapsed_time));
      } onFailure { e =>
        // should never get here since this filter is meant to be after the exception barrier
        logger.info(logFormatter.formatException(request, e, elapsed()))
      }
    }
  }
}
