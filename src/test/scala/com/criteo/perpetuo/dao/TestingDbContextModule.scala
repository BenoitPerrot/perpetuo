package com.criteo.perpetuo.dao

import java.util.UUID

import com.criteo.perpetuo.app.{AppConfig, DbContextModule}
import com.criteo.perpetuo.dao.drivers.UrlBuilders._
import com.criteo.perpetuo.dao.drivers.{FixedH2Driver, InMemory}
import org.h2.jdbcx.JdbcConnectionPool


class TestingDbContextModule(dbConfig: AppConfig) extends DbContextModule(dbConfig) {
  lazy val dataSource: JdbcConnectionPool = {
    val dbID = UUID.randomUUID().toString.replace("-", "")
    JdbcConnectionPool.create(FixedH2Driver.buildUrl(InMemory(), s"${dbConfig.get[String]("name")}_$dbID", dbConfig.get[String]("schema")), "sa", "")
  }

  override def dataSourceProvider: JdbcConnectionPool = dataSource
}
