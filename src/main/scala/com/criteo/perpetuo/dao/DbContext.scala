package com.criteo.perpetuo.dao

import javax.sql.DataSource

import slick.driver.JdbcDriver

class DbContext(val driver: JdbcDriver, dataSource: DataSource) {

  import driver.api._

  lazy val db: driver.backend.DatabaseDef = Database.forDataSource(dataSource)
}
