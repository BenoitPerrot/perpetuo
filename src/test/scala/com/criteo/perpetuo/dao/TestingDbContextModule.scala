package com.criteo.perpetuo.dao

import java.util.UUID

import com.criteo.perpetuo.app.DbContextModule
import com.criteo.perpetuo.config.AppConfig
import org.h2.jdbcx.JdbcConnectionPool
import slick.jdbc.JdbcBackend._


class TestingDbContextModule(dbConfig: AppConfig) extends DbContextModule(dbConfig) {
  lazy val database: Database = {
    val dbID = UUID.randomUUID().toString.replace("-", "")
    Database.forDataSource(JdbcConnectionPool.create(s"jdbc:h2:mem:Deployment_DB_$dbID;DB_CLOSE_DELAY=-1;MULTI_THREADED=1;INIT=CREATE SCHEMA IF NOT EXISTS dbo;IGNORECASE=true;DATABASE_TO_UPPER=false", "sa", ""))
  }

  override def databaseProvider: Database = database
}
