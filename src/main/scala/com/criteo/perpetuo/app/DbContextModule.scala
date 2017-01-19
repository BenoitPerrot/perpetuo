package com.criteo.perpetuo.app

import javax.sql.DataSource

import com.criteo.datasource.{DataSourceFactory, DataSourceFactoryBuilder}
import com.criteo.ds.prm.{DataSourceProxymitySpecifier, DatasourceIntent}
import com.criteo.perpetuo.dao.drivers.UrlBuilders._
import com.criteo.perpetuo.dao.drivers.{DriverByName, InMemory}
import com.criteo.sdk.discovery.prmdb.Resource
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.typesafe.config.Config
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.driver.{JdbcDriver, JdbcProfile}

import scala.util.Try


class DbContextModule(dbConfig: Config) extends TwitterModule {

  val driverName: String = dbConfig.getString("driver")
  val driver: JdbcDriver = DriverByName.get(driverName)

  private lazy val dataSource = {
    if (driverName == "net.sourceforge.jtds.jdbc.Driver") {
      getDataSourceViaPrm()
    } else {
      val username = dbConfig.getString("username")
      val password = Try(dbConfig.getString("password")).getOrElse("")
      getInMemoryDataSource(username, password)
    }
  }

  // For being overridden, eg for testing
  def dataSourceProvider: DataSource = dataSource

  @Singleton
  @Provides
  def providesDbProfile: JdbcProfile = {
    driver
  }

  @Singleton
  @Provides
  def providesDbDataSource: DataSource = {
    dataSourceProvider
  }

  private def getDataSourceViaPrm(): DataSource = {
    DataSourceFactoryBuilder.createFactoryAutoDetect()
      .buildUsingKerberosAuthentication()
      .getDataSourceForDatabase(Resource.Deployment_DB, DatasourceIntent.ReadWrite, DataSourceProxymitySpecifier.GlobalAllowed)
  }

  private def getInMemoryDataSource(username: String, password: String): DataSource = {
    val dbName = dbConfig.getString("name")
    val schemaName = dbConfig.getString("schema")
    val jdbcUrl = driver.buildUrl(InMemory(), dbName, schemaName)

    val maxPoolSize = Try(dbConfig.getString("poolMaxSize").toInt).getOrElse(10)
    val minimumIdle = Try(dbConfig.getString("poolMinSize").toInt).getOrElse(maxPoolSize)
    val idleTimeout = Try(dbConfig.getString("idleTimeout").toInt).getOrElse(600000)
    val connectionTimeout = Try(dbConfig.getString("connectionTimeout").toInt).getOrElse(30000)

    logger.info(jdbcUrl)
    new HikariDataSource(new HikariConfig() {
      {
        setJdbcUrl(jdbcUrl)
        setUsername(username)
        setPassword(password)
        setDriverClassName(driverName)
        setInitializationFailFast(false) // Do not fail if the DB does not exist when initializing. The only case is migration tests.
        setConnectionTestQuery("SELECT 1;") // Do not rely on Jdbc isValid to check the connection. Some drivers do not implement it.
        setPoolName(s"${dbName}_ConnectionPool")
        setMinimumIdle(minimumIdle)
        setMaximumPoolSize(maxPoolSize)
        setIdleTimeout(idleTimeout)
        setConnectionTimeout(connectionTimeout)
      }
    })
  }
}
