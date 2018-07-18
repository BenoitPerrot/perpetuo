package com.criteo.perpetuo.dao

import com.criteo.perpetuo.{TestDb, TestHelpers}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ProductBinderSpec
  extends TestHelpers
    with ProductBinder
    with TestDb {

  import dbContext.profile.api._

  test("Inserted product should have an active status") {
    await({
      dbContext.db.run(productQuery.delete)
      for {
        product1 <- upsertProduct("product1")
        product2 <- upsertProduct("product2", active = false)
      } yield {
        (product1.active, product2.active)
      }
    }) shouldBe (true, false)
  }

  test("Existing product's active status is updated when upserting") {
    await({
      dbContext.db.run(productQuery.delete)
      for {
        _ <- upsertProduct("product1")
        product <- upsertProduct("product1", active = false)
      } yield {
        product.active
      }
    }) shouldBe false
  }

  test("Return all inserted products") {
    val nameSet = Set("app1", "app2", "app3")
    await({
      dbContext.db.run(productQuery.delete)
      val futures = nameSet.map(name => upsertProduct(name))
      Future.sequence(futures).map(_ =>
        getProducts.map(productList => {
          val x = productList.map(_.name).toSet
          assert(x == nameSet)
        })
      )
    })
  }

  test("Bulk update the status of products") {
    val existingProductNames = List("app1", "app2", "app3")
    val postedProductNames = List("app3", "app4")

    await({
      for {
        _ <- dbContext.db.run(productQuery.delete)
        _ <- Future.sequence(existingProductNames.map(name => upsertProduct(name)))
        products <- setActiveProducts(postedProductNames)
      } yield {
        products.map(product => (product.name, product.active)).toSet
      }
    }) shouldBe Set(("app1", false), ("app2", false), ("app4", true))
  }
}
