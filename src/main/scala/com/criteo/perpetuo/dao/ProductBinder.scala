package com.criteo.perpetuo.dao

import com.criteo.perpetuo.model.Product
import slick.jdbc.TransactionIsolation

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


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

  def insertProductIfNotExists(productName: String): Future[Product] = {
    val q = productQuery.filter(_.name === productName).result.flatMap(existing =>
      existing.headOption.map(product =>
        DBIO.successful(product.toProduct)
      ).getOrElse(
        (productQuery.returning(productQuery.map(_.id)) += ProductRecord(None, productName)).map(Product(_, productName))
      )
    )
    dbContext.db.run(q.transactionally.withTransactionIsolation(TransactionIsolation.Serializable))
  }

  def getProductNames: Future[Seq[String]] = {
    dbContext.db.run(productQuery.result).map(_.map(_.name))
  }

  def findProductByName(name: String): Future[Option[Product]] = {
    dbContext.db.run(productQuery.filter(_.name === name).result).map(_.headOption.map(_.toProduct))
  }
}
