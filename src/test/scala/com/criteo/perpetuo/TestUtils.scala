package com.criteo.perpetuo

import com.criteo.perpetuo.app.{AppConfig, DbContext}
import com.criteo.perpetuo.dao.{DbContextProvider, TestingDbContextModule}

import scala.io.Source


object TestUtils {
  def getResourceAsString(resourceFileName: String): String = {
    val cls = Class.forName(Thread.currentThread.getStackTrace.apply(2).getClassName)
    Source.fromURL(cls.getResource(resourceFileName)).mkString
  }
}


trait TestDb extends DbContextProvider {
  lazy val dbTestModule = new TestingDbContextModule(AppConfig.withEnv("test").db)
  lazy val dbContext: DbContext = dbTestModule.providesDbContext
}
