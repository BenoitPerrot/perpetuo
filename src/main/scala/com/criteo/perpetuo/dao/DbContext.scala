package com.criteo.perpetuo.dao

import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile


class DbContext(val driver: JdbcProfile, val db: Database)
