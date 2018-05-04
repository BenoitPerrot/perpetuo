package com.criteo.perpetuo.app

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.drivers.DriverByName
import com.criteo.perpetuo.dao.{DbContext, Schema}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile
import slick.util.AsyncExecutor

class DbContextModule(val config: Config) extends TwitterModule {
  val threadPrefix = "DB"

  val driverName: String = config.getString("driver.name")
  val driverProfile: String = config.getString("driver.profile")
  val driver: JdbcProfile = DriverByName.get(driverProfile)
  val numThreadsAndQueueSize: Option[(Int, Int)] = {
    val numThreads = config.tryGet[Int]("numThreads")
    val queueSize = config.tryGet[Int]("queueSize")
    if (numThreads.isDefined ^ queueSize.isDefined)
      throw new ConfigException.Generic("db.numThreads and db.queueSize: either both or none must be provided")
    numThreads.map((_, queueSize.get))
  }

  private def toHikariConfig(jdbcUrl: String, config: Config) = {
    val maxPoolSize = config.getOrElse("poolMaxSize", 10)
    val minimumIdle = config.getOrElse("poolMinSize", maxPoolSize)
    val idleTimeout = config.getOrElse("idleTimeout", 600000)
    val connectionTimeout = config.getOrElse("connectionTimeout", 30000)
    val validationTimeout = config.getOrElse("validationTimeout", 5000)

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
      ),
      None,
      numThreadsAndQueueSize.map { case (numThreads, queueSize) =>
        AsyncExecutor(threadPrefix, numThreads, queueSize)
      }.getOrElse(
        AsyncExecutor.default(threadPrefix)
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
