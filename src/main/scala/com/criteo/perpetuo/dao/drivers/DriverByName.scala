package com.criteo.perpetuo.dao.drivers

import slick.jdbc.{H2Profile, JdbcProfile, SQLServerProfile}


object DriverByName {
  def get(name: String): JdbcProfile = name match {
    case "h2" => H2Profile
    case "mssql" => SQLServerProfile
  }
}
