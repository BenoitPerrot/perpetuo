package com.criteo.perpetuo.app

import com.twitter.finagle.filter.LogFormatter
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finatra.filters.MergedFilter
import com.twitter.finatra.http.filters.{ExceptionMappingFilter, HttpResponseFilter, StatsFilter}
import com.twitter.inject.Logging
import com.twitter.util.{Future, Stopwatch}
import javax.inject.{Inject, Singleton}


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
    if (!isInfoEnabled || request.method.toString == "GET") {
      service(request)
    }
    else {
      val elapsed = Stopwatch.start()
      service(request) onSuccess { response =>
        if (response.statusCode != 200 || request.method.toString == "PUT")
          info(logFormatter.format(request, response, elapsed()))
      } onFailure { e =>
        // should never get here since this filter is meant to be after the exception barrier
        info(logFormatter.formatException(request, e, elapsed()))
      }
    }
  }
}
