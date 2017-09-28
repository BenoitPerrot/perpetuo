package com.criteo.perpetuo

import com.criteo.perpetuo.config.AppConfigProvider
import com.criteo.perpetuo.dao.{DbContext, DbContextProvider, TestingDbContextModule}

import scala.io.Source


object TestUtils {
  def getResourceAsString(resourceFileName: String): String = {
    val cls = Class.forName(Thread.currentThread.getStackTrace.apply(2).getClassName)
    Source.fromURL(cls.getResource(resourceFileName)).mkString
  }
}


trait TestDb extends DbContextProvider {
  lazy val dbTestModule = new TestingDbContextModule(AppConfigProvider.config.getConfig("db"))
  lazy val dbContext: DbContext = dbTestModule.providesDbContext
}
