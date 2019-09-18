package org.bd.cart

import org.apache.commons.lang3.RandomUtils

import scala.collection.mutable

object ProductDB {
  val products = List(
    Map("id" -> "Apple iPhone 7 Plus 32 GB", "category" -> "Apple Iphone", "price" -> "479"),
    Map("id" -> "Apple iPhone 7 Plus 64 GB", "category" -> "Apple Iphone", "price" -> "679"),
    Map("id" -> "Apple iPhone 7 Plus 128 GB", "category" -> "Apple Iphone", "price" -> "879"),
    Map("id" -> "Apple iPhone 8 Plus 32 GB", "category" -> "Apple Iphone", "price" -> "579"),
    Map("id" -> "Apple iPhone 8 Plus 64 GB", "category" -> "Apple Iphone", "price" -> "779"),
    Map("id" -> "Apple iPhone 8 Plus 128 GB", "category" -> "Apple Iphone", "price" -> "979"),
    Map("id" -> "Apple iPhone X Plus 32 GB", "category" -> "Apple Iphone", "price" -> "699"),
    Map("id" -> "Apple iPhone X Plus 64 GB", "category" -> "Apple Iphone", "price" -> "899"),
    Map("id" -> "Apple iPhone X Plus 128 GB", "category" -> "Apple Iphone", "price" -> "1099"),
    Map("id" -> "Galaxy Note 8", "category" -> "Samsung Note", "price" -> "499"),
    Map("id" -> "Galaxy Note 9", "category" -> "Samsung Note", "price" -> "699"),
    Map("id" -> "Galaxy Note 10", "category" -> "Samsung Note", "price" -> "899"),
    Map("id" -> "Galaxy S7 Plus", "category" -> "Samsung S", "price" -> "599"),
    Map("id" -> "Galaxy S8 Plus", "category" -> "Samsung S", "price" -> "699"),
    Map("id" -> "Galaxy S9 Plus", "category" -> "Samsung S", "price" -> "799"),
    Map("id" -> "Apple Mac Pro 2016", "category" -> "Apple Mac", "price" -> "679"),
    Map("id" -> "Apple Mac Pro 2017", "category" -> "Apple Mac", "price" -> "779"),
    Map("id" -> "Apple Mac Pro 2018", "category" -> "Apple Mac", "price" -> "879"),
    Map("id" -> "Apple Mac Pro 2019", "category" -> "Apple Mac", "price" -> "979"),
    Map("id" -> "HP Pavilion", "category" -> "HP Laptop", "price" -> "338"),
    Map("id" -> "2019 HP Flagship", "category" -> "HP Laptop", "price" -> "422"),
    Map("id" -> "HP Touchscreen", "category" -> "HP Laptop", "price" -> "522")
  )

  val priorities = new mutable.ListBuffer[Integer]
  var totalP = 0

   products.foreach(_ => {
     val p = RandomUtils.nextInt(0, priorities.size * 10000)
     priorities += p
     totalP += p
   })

  def nextRandom(): Map[String, String] = {
    val p = RandomUtils.nextInt(0, totalP)
    var acc = 0
    for (idx <- 0 to products.size) {
      acc += priorities(idx)
      if (acc > p) {
        return products(idx)
      }
    }
    products(RandomUtils.nextInt(0, products.size))
  }
}
