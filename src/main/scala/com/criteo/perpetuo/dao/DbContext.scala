package com.criteo.perpetuo.dao

import slick.driver.JdbcDriver
import slick.jdbc.JdbcBackend.Database

class DbContext(val driver: JdbcDriver, val db: Database)