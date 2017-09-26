package com.criteo.perpetuo.dao.drivers

import freeslick.MSSQLServerProfile
import slick.driver.JdbcDriver


object DriverByName {
  def get(name: String): JdbcDriver = name match {
    case "h2" => FixedH2Driver
    case "mssql" => MSSQLServerProfile
  }
}
