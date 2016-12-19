package com.criteo.perpetuo.dao

import java.util.UUID

import com.criteo.perpetuo.app.DbContextModule
import com.criteo.perpetuo.dao.drivers.UrlBuilders._
import com.criteo.perpetuo.dao.drivers.{FixedH2Driver, InMemory}
import com.typesafe.config.Config
import org.h2.jdbcx.JdbcConnectionPool


class TestingDbContextModule(dbConfig: Config) extends DbContextModule(dbConfig) {
  lazy val dataSource: JdbcConnectionPool = {
    val dbID = UUID.randomUUID().toString.replace("-", "")
    JdbcConnectionPool.create(FixedH2Driver.buildUrl(InMemory(), s"${dbConfig.getString("name")}_$dbID", dbConfig.getString("schema")), "sa", "")
  }

  override def dataSourceProvider: JdbcConnectionPool = dataSource
}
