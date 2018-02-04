package com.criteo.perpetuo.app

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.drivers.DriverByName
import com.criteo.perpetuo.dao.{DbContext, Schema}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.driver.JdbcDriver
import slick.jdbc.JdbcBackend.Database

class DbContextModule(val config: Config) extends TwitterModule {
  val driverName: String = config.getString("driver.name")
  val driverProfile: String = config.getString("driver.profile")
  val driver: JdbcDriver = DriverByName.get(driverProfile)

  private def toHikariConfig(jdbcUrl: String, config: Config) = {
    val maxPoolSize = config.tryGet[Int]("poolMaxSize").getOrElse(10)
    val minimumIdle = config.tryGet[Int]("poolMinSize").getOrElse(maxPoolSize)
    val idleTimeout = config.tryGet[Int]("idleTimeout").getOrElse(600000)
    val connectionTimeout = config.tryGet[Int]("connectionTimeout").getOrElse(30000)
    val validationTimeout = config.tryGet[Int]("validationTimeout").getOrElse(5000)

    val hikariConfig = new HikariConfig
    hikariConfig.setJdbcUrl(jdbcUrl)
    hikariConfig.setDriverClassName(driverName)
    hikariConfig.setConnectionTestQuery("SELECT 1;") // Do not rely on Jdbc isValid to check the connection. Some drivers do not implement it.
    hikariConfig.setPoolName(s"${jdbcUrl}_ConnectionPool")
    hikariConfig.setMinimumIdle(minimumIdle)
    hikariConfig.setMaximumPoolSize(maxPoolSize)
    hikariConfig.setIdleTimeout(idleTimeout)
    hikariConfig.setConnectionTimeout(connectionTimeout)
    hikariConfig.setValidationTimeout(validationTimeout)

    hikariConfig
  }

  private lazy val database =
    Database.forDataSource(
      new HikariDataSource(
        toHikariConfig(config.getString("jdbcUrl"), config.tryGetConfig("hikari").getOrElse(ConfigFactory.empty()))
      )
    )

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
