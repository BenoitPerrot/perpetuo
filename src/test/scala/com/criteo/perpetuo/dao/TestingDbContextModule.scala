package com.criteo.perpetuo.dao

import java.util.UUID

import com.criteo.perpetuo.app.DbContextModule
import com.typesafe.config.Config
import slick.jdbc.JdbcBackend.Database

class TestingDbContextModule(dbConfig: Config) extends DbContextModule(dbConfig) {
  lazy val jdbcUrl: String = {
    val dbID = UUID.randomUUID().toString.replace("-", "")
    s"jdbc:h2:mem:Deployment_DB_$dbID;DB_CLOSE_DELAY=-1;MULTI_THREADED=1;INIT=CREATE SCHEMA IF NOT EXISTS dbo;IGNORECASE=true;DATABASE_TO_UPPER=false"
  }

  override def databaseProvider: Database = Database.forURL(jdbcUrl, driver = "org.h2.Driver", user = "sa", password = "", executor = executor)
}
