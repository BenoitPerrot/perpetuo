package com.criteo.perpetuo.dao

import com.criteo.perpetuo.{TestDb, TestHelpers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ProductBinderSpec
  extends TestHelpers
    with ProductBinder
    with TestDb {

  import dbContext.profile.api._

  test("Inserted product should have an active status") {
    await(
      for {
        _ <- dbContext.db.run(productQuery.delete)
        product1 <- upsertProduct("product1")
        product2 <- upsertProduct("product2", active = false)
      } yield {
        product1.active shouldBe true
        product2.active shouldBe false
      }
    )
  }

  test("Existing product's active status is updated when upserting") {
    await(
      for {
        _ <- dbContext.db.run(productQuery.delete)
        _ <- upsertProduct("product1")
        product <- upsertProduct("product1", active = false)
      } yield {
        product.active shouldBe false
      }
    )
  }

  test("Return all inserted products") {
    val nameSet = Set("app1", "app2", "app3")
    await(
      for {
        _ <- dbContext.db.run(productQuery.delete)
        _ <- Future.traverse(nameSet)(upsertProduct(_))
        products <- getProducts
      } yield {
        products.map(_.name).toSet shouldEqual nameSet
      }
    )
  }

  test("Bulk update the status of products") {
    val existingProductNames = List("app1", "app2", "app3")
    val postedProductNames = List("app3", "app4")
    await(
      for {
        _ <- dbContext.db.run(productQuery.delete)
        _ <- Future.sequence(existingProductNames.map(name => upsertProduct(name)))
        products <- setActiveProducts(postedProductNames)
      } yield {
        products.map(product => (product.name, product.active)) shouldEqual
          Set(("app1", false), ("app2", false), ("app4", true))
      }
    )
  }
}
