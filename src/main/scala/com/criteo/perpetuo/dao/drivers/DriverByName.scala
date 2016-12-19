package com.criteo.perpetuo.dao.drivers

import freeslick.MSSQLServerProfile
import slick.driver.JdbcDriver


object DriverByName {
  def get(name: String): JdbcDriver = name match {
    case "org.h2.Driver" => FixedH2Driver
    case "net.sourceforge.jtds.jdbc.Driver" => MSSQLServerProfile
  }
}
