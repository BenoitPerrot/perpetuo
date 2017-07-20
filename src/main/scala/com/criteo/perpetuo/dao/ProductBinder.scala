package com.criteo.perpetuo.dao

import java.sql.SQLException

import com.criteo.perpetuo.model.Product

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class ProductCreationConflict(productName: String, private val cause: Throwable = null)
  extends Exception(s"Name `$productName` is already used", cause)


private[dao] case class ProductRecord(id: Option[Int], name: String) {
  def toProduct: Product = {
    assert(id.isDefined)
    Product(id.get, name)
  }
}


trait ProductBinder extends TableBinder {
  this: DbContextProvider =>

  import dbContext.driver.api._

  class ProductTable(tag: Tag) extends Table[ProductRecord](tag, "product") {
    def id = column[Int]("id", O.AutoInc)
    protected def pk = primaryKey(id)

    def name = column[String]("name", O.SqlType("nvarchar(128)")) // the name is not the pk, in order to easily support renaming without losing history
    protected def nameIdx = index(name, unique = true)

    def * = (id.?, name) <> (ProductRecord.tupled, ProductRecord.unapply)
  }

  val productQuery = TableQuery[ProductTable]

  def insert(productName: String): Future[Product] = {
    dbContext.db.run((productQuery returning productQuery.map(_.id)) += ProductRecord(None, productName)).map(
      Product(_, productName)
    ).recover {
      case e: SQLException if e.getMessage.contains("nique index") =>
        // there is no specific exception type if the name is already used but the error message starts with
        // - if H2: Unique index or primary key violation: "ix_product_name ON PUBLIC.""product""(""name"") VALUES ('my product', 1)"
        // - if SQLServer: Cannot insert duplicate key row in object 'dbo.product' with unique index 'ix_product_name'
        throw ProductCreationConflict(productName, e)
    }
  }

  def getProductNames: Future[Seq[String]] = {
    dbContext.db.run(productQuery.result).map(_.map(_.name))
  }

  def findProductByName(name: String): Future[Option[Product]] = {
    dbContext.db.run(productQuery.filter(_.name === name).result).map(_.headOption.map(_.toProduct))
  }
}
