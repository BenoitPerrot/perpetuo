package com.criteo.perpetuo.app

import javax.sql.DataSource

import com.criteo.datasource.DataSourceFactoryBuilder
import com.criteo.ds.prm.{DataSourceProxymitySpecifier, DatasourceIntent}
import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.dao.drivers.DriverByName
import com.criteo.perpetuo.dao.{DbContext, Schema}
import com.criteo.sdk.discovery.prmdb.Resource
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import slick.driver.JdbcDriver
import slick.jdbc.JdbcBackend.Database

import scala.util.Try

class DbContextModule(val dbConfig: AppConfig) extends TwitterModule {
  val driverName: String = dbConfig.get("driver")
  val driver: JdbcDriver = DriverByName.get(driverName)

  private lazy val database =
    if (driverName == "net.sourceforge.jtds.jdbc.Driver") {
      Database.forDataSource(obtainDataSourceFromPrm())
    } else {
      val jdbcUrl = dbConfig.get[String]("jdbcUrl")
      val username = Try(dbConfig.get[String]("username")).getOrElse(null)
      val password = Try(dbConfig.get[String]("password")).getOrElse(null)
      Database.forURL(jdbcUrl, driver=driverName, user=username, password=password)
    }

  // For being overridden, eg for testing
  def databaseProvider: Database = database

  @Singleton
  @Provides
  def providesDbContext: DbContext = {
    val dbContext = new DbContext(driver, databaseProvider)
    if (dbConfig.get("ephemeral"))
      new Schema(dbContext).createTables()
    dbContext
  }

  private def obtainDataSourceFromPrm(): DataSource = {
    DataSourceFactoryBuilder.createFactoryAutoDetect()
      .buildUsingKerberosAuthentication()
      .getDataSourceForDatabase(Resource.Deployment_DB, DatasourceIntent.ReadWrite, DataSourceProxymitySpecifier.GlobalAllowed)
  }

}
