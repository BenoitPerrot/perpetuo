package com.criteo.perpetuo.app

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.drivers.DriverByName
import com.criteo.perpetuo.dao.{DbContext, Schema}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.typesafe.config.Config
import slick.driver.JdbcDriver
import slick.jdbc.JdbcBackend.Database

class DbContextModule(val config: Config) extends TwitterModule {
  val driverName: String = config.getString("driver.name")
  val driverProfile: String = config.getString("driver.profile")
  val driver: JdbcDriver = DriverByName.get(driverProfile)

  private lazy val database = {
    val jdbcUrl = config.getString("jdbcUrl")
    val username = config.tryGet[String]("username").orNull
    val password = config.tryGet[String]("password").orNull
    Database.forURL(jdbcUrl, driver=driverName, user=username, password=password)
  }

  // For being overridden, eg for testing
  def databaseProvider: Database = database

  @Singleton
  @Provides
  def providesDbContext: DbContext = {
    val dbContext = new DbContext(driver, databaseProvider)
    if (config.getBoolean("ephemeral"))
      new Schema(dbContext).createTables()
    dbContext
  }
}
