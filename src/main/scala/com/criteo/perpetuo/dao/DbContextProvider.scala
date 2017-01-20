package com.criteo.perpetuo.dao

import com.criteo.perpetuo.app.DbContext


trait DbContextProvider {
  val dbContext: DbContext
}
