package com.criteo.perpetuo.dao.drivers


abstract class Location()

case class Remote(host: String, port: Int) extends Location

case class InMemory() extends Location
