package com.criteo.perpetuo.app.dao

import slick.driver.JdbcProfile

trait ProfileProvider {
  val profile: JdbcProfile
}
