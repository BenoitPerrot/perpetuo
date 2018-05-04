package com.criteo.perpetuo.dao.drivers

import slick.jdbc.SQLServerProfile
import slick.jdbc.JdbcProfile


object DriverByName {
  def get(name: String): JdbcProfile = name match {
    case "h2" => FixedH2Driver
    case "mssql" => SQLServerProfile
  }
}
