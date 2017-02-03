package com.criteo.perpetuo

import com.criteo.perpetuo.app.AppConfig
import com.criteo.perpetuo.dao.{DbContext, DbContextProvider, TestingDbContextModule}

import scala.io.Source


object TestUtils {
  def getResourceAsString(resourceFileName: String): String = {
    val cls = Class.forName(Thread.currentThread.getStackTrace.apply(2).getClassName)
    Source.fromURL(cls.getResource(resourceFileName)).mkString
  }
}


trait TestDb extends DbContextProvider {
  lazy val dbTestModule = new TestingDbContextModule(AppConfig.db)
  lazy val dbContext: DbContext = dbTestModule.providesDbContext
}
