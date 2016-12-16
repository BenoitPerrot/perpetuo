package com.criteo.perpetuo.dao

import slick.driver.JdbcProfile

trait ProfileProvider {
  val profile: JdbcProfile
}
