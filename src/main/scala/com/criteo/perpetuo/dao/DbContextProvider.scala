package com.criteo.perpetuo.dao


trait DbContextProvider {
  val dbContext: DbContext
}
