package com.criteo.perpetuo.app

import com.criteo.perpetuo.config.AppConfig
import com.criteo.perpetuo.dao.drivers.DriverByName
import com.criteo.perpetuo.dao.{DbContext, Schema}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import slick.driver.JdbcDriver
import slick.jdbc.JdbcBackend.Database

import scala.util.Try

class DbContextModule(val dbConfig: AppConfig) extends TwitterModule {
  val driverName: String = dbConfig.get("driver.name")
  val driverProfile: String = dbConfig.get("driver.profile")
  val driver: JdbcDriver = DriverByName.get(driverProfile)

  private lazy val database = {
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
}
