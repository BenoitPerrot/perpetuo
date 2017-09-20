package com.criteo.perpetuo.dao

import java.util.UUID

import com.criteo.perpetuo.app.DbContextModule
import com.criteo.perpetuo.config.AppConfig
import org.h2.jdbcx.JdbcConnectionPool


class TestingDbContextModule(dbConfig: AppConfig) extends DbContextModule(dbConfig) {
  lazy val dataSource: JdbcConnectionPool = {
    val dbID = UUID.randomUUID().toString.replace("-", "")
    JdbcConnectionPool.create(s"jdbc:h2:mem:Deployment_DB_$dbID;DB_CLOSE_DELAY=-1;MULTI_THREADED=1;INIT=CREATE SCHEMA IF NOT EXISTS dbo;IGNORECASE=true;DATABASE_TO_UPPER=false", "sa", "")
  }

  override def dataSourceProvider: JdbcConnectionPool = dataSource
}
