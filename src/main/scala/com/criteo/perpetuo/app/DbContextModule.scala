package com.criteo.perpetuo.app

import com.criteo.perpetuo.config.ConfigSyntacticSugar._
import com.criteo.perpetuo.dao.{DbContext, Schema}
import com.google.inject.{Provides, Singleton}
import com.twitter.inject.TwitterModule
import com.typesafe.config.{Config, ConfigFactory}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.{H2Profile, JdbcProfile, SQLServerProfile}
import slick.util.AsyncExecutor

class DbContextModule(val config: Config) extends TwitterModule {
  val threadPrefix = "DB"

  val driverName: String = config.getString("driver.name")
  val profile: JdbcProfile = config.getString("driver.profile") match {
    case "h2" => H2Profile
    case "mssql" => SQLServerProfile
  }

  val numThreads: Int = config.getIntOrElse("numThreads", 20)
  val queueSize: Int = config.getIntOrElse("queueSize", 1000)

  private def toHikariConfig(jdbcUrl: String, config: Config) = {
    val maxPoolSize = config.getIntOrElse("poolMaxSize", 10)
    val minimumIdle = config.getIntOrElse("poolMinSize", maxPoolSize)
    val idleTimeout = config.getIntOrElse("idleTimeout", 600000)
    val connectionTimeout = config.getIntOrElse("connectionTimeout", 30000)
    val validationTimeout = config.getIntOrElse("validationTimeout", 5000)

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

  protected def executor = AsyncExecutor(threadPrefix, numThreads, numThreads, queueSize, numThreads)

  // For being overridden, eg for testing
  protected def databaseProvider: Database = Database.forDataSource(
    new HikariDataSource(
      toHikariConfig(config.getString("jdbcUrl"), config.tryGetConfig("hikari").getOrElse(ConfigFactory.empty()))
    ),
    Some(numThreads),
    executor
  )

  @Singleton
  @Provides
  def providesDbContext: DbContext = {
    val dbContext = new DbContext(profile, databaseProvider)
    if (config.getBoolean("ephemeral"))
      new Schema(dbContext).createTables()
    dbContext
  }
}
