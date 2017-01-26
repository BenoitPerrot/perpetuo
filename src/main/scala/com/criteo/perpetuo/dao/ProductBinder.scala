package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.Product

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait ProductBinder extends TableBinder {
  this: DbContextProvider =>

  import dbContext.driver.api._

  class ProductTable(tag: Tag) extends Table[Product](tag, "product") {
    def id = column[Int]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def name = column[String]("name", O.SqlType("nchar(128)"))
    protected def nameIdx = index(name, unique = true)

    def * = (id.?, name) <> (Product.tupled, Product.unapply)
  }

  val productQuery = TableQuery[ProductTable]

  def insert(p: Product): Future[Int] = {
    dbContext.db.run((productQuery returning productQuery.map(_.id)) += p)
  }

  def findProductById(id: Int): Future[Option[Product]] = {
    dbContext.db.run(productQuery.filter(_.id === id).result).map(_.headOption)
  }

  def findProductByName(name: String): Future[Option[Product]] = {
    dbContext.db.run(productQuery.filter(_.name === name).result).map(_.headOption)
  }
}
