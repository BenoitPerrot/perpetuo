package com.criteo.perpetuo.dao.drivers

import freeslick.MSSQLServerProfile
import slick.driver.JdbcDriver


object UrlBuilders {
  private def buildH2Url(location: Location, dbName: String, schemaName: String): String = {
    location match {
      case InMemory() =>
        s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1;MULTI_THREADED=1;INIT=CREATE SCHEMA IF NOT EXISTS $schemaName;IGNORECASE=true;DATABASE_TO_UPPER=false"
      case Remote(host, port) =>
        s"jdbc:h2:tcp://$host:$port/mem:$dbName;MULTI_THREADED=0;INIT=CREATE SCHEMA IF NOT EXISTS $schemaName;IGNORECASE=true;DATABASE_TO_UPPER=false"
    }
  }

  private def buildSqlServerUrl(location: Remote, dbName: String, schemaName: String): String = {
    // http://jtds.sourceforge.net/faq.html
    // SQL Server can run multiple so-called "named instances" (i.e. different server instances, running on different TCP ports) on the same machine.
    // When using Microsoft tools, selecting one of these instances is made by using "<host>\<instance_name>" instead of the usual "<host>".
    // With jTDS you will have to split the two and use the instance name as a property.
    val split = location.host.split("\\")
    val host = split.head.trim
    val instanceName = split.tail.headOption.getOrElse("").trim

    val instanceNameProperty = if (instanceName.nonEmpty) s";instance=$instanceName" else ""

    val dbNameProperty = if (dbName != null && dbName.nonEmpty) s";DatabaseName=$dbName" else ""

    s"jdbc:jtds:sqlserver://$host:${location.port}$instanceNameProperty$dbNameProperty"
  }

  implicit class H2UrlBuilder(val driver: JdbcDriver) {
    def buildUrl(location: Location, dbName: String, schemaName: String): String =
      driver match {
        case FixedH2Driver => buildH2Url(location, dbName, schemaName)
        case MSSQLServerProfile => buildSqlServerUrl(location.asInstanceOf[Remote], dbName, schemaName)
      }
  }
}
