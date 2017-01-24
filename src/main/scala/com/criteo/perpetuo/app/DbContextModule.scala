package com.criteo.perpetuo.app

import javax.sql.DataSource

import com.criteo.datasource.DataSourceFactoryBuilder
import com.criteo.ds.prm.{DataSourceProxymitySpecifier, DatasourceIntent}
import com.criteo.perpetuo.dao.Schema
import com.criteo.perpetuo.dao.drivers.UrlBuilders._
import com.criteo.perpetuo.dao.drivers.{DriverByName, InMemory}
import com.criteo.sdk.discovery.prmdb.Resource
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.driver.JdbcDriver

import scala.util.Try


class DbContext(val driver: JdbcDriver, dataSource: DataSource) {
  import driver.api._
  lazy val db: driver.backend.DatabaseDef = Database.forDataSource(dataSource)
}


class DbContextModule(val dbConfig: AppConfig) extends TwitterModule {
  val driverName: String = dbConfig.get("driver")
  val driver: JdbcDriver = DriverByName.get(driverName)

  private lazy val dataSource = {
    if (driverName == "net.sourceforge.jtds.jdbc.Driver") {
      obtainDataSourceFromPrm()
    } else {
      val username = dbConfig.get[String]("username")
      val password = Try(dbConfig.get[String]("password")).getOrElse("")
      createInMemoryDataSource(username, password)
    }
  }

  // For being overridden, eg for testing
  def dataSourceProvider: DataSource = dataSource

  @Singleton
  @Provides
  def providesDbContext: DbContext = {
    val dbContext = new DbContext(driver, dataSourceProvider)
    if (dbConfig.get("ephemeral")) {
      // running in development mode
      new Schema(dbContext).createTables()
    }
    dbContext
  }

  private def obtainDataSourceFromPrm(): DataSource = {
    DataSourceFactoryBuilder.createFactoryAutoDetect()
      .buildUsingKerberosAuthentication()
      .getDataSourceForDatabase(Resource.Deployment_DB, DatasourceIntent.ReadWrite, DataSourceProxymitySpecifier.GlobalAllowed)
  }

  private def createInMemoryDataSource(username: String, password: String): DataSource = {
    val dbName = dbConfig.get[String]("name")
    val schemaName = dbConfig.get[String]("schema")
    val jdbcUrl = driver.buildUrl(InMemory(), dbName, schemaName)

    val maxPoolSize = Try(dbConfig.get[Int]("poolMaxSize")).getOrElse(10)
    val minimumIdle = Try(dbConfig.get[Int]("poolMinSize")).getOrElse(maxPoolSize)
    val idleTimeout = Try(dbConfig.get[Int]("idleTimeout")).getOrElse(600000)
    val connectionTimeout = Try(dbConfig.get[Int]("connectionTimeout")).getOrElse(30000)

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
