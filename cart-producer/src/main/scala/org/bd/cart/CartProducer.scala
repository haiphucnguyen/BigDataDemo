package org.bd.cart

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.mekong.dto._
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

object CartProducer {

  val logger = LoggerFactory.getLogger(classOf[Streamer])

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val streamer = new Streamer(conf)
    val timeRangeInput = conf.getInt("time-range")
    val delayTime = conf.getInt("message-delay")
    var shutdown = false

    val mapper = new ObjectMapper()
    mapper.registerModules(DefaultScalaModule, new JavaTimeModule())

    new Thread(new Runnable {
      override def run(): Unit = {
        while (!shutdown) {
          val items = new ArrayBuffer[Order]()
          for (_ <- 0 to RandomUtils.nextInt(2, 6)) {
            val product = ProductDB.nextRandom()
            val item = Order(
              UUID.randomUUID().toString,
              product("id"),
              product("category"),
              product("price").toDouble,
              RandomUtils.nextInt(1, 5)
            )
            items += item
          }

          var timeRange = RandomUtils.nextInt(1, timeRangeInput)
          timeRange = if (RandomUtils.nextBoolean()) timeRange else -timeRange
          val cartTime = Instant.now().plus(timeRange, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS)
          val cart = Cart(
            UUID.randomUUID().toString,
            RandomStringUtils.randomAlphabetic(10),
            cartTime.getEpochSecond,
            cartTime.plus(3, ChronoUnit.DAYS).getEpochSecond,
            items.toList
          )

          streamer.sendCart(mapper.writeValueAsString(cart), cart.cardId.toString)

          val address = ZipCodeDB.nextRandomAddress()
          val shipping =
            ShippingAddress(
              cart.cardId,
              address("address"),
              "city",
              address("zip"),
              address("state")
            )
          streamer.sendShipping(mapper.writeValueAsString(shipping), shipping.cartId.toString)

          while (streamer.isOverload()) {
            logger.info("To much message, waiting...")
            Thread.sleep(50)
          }

          Thread.sleep(delayTime)
        }
      }
    }).start()

    val input = scala.io.StdIn.readLine()
    shutdown = true
    while (streamer.isBusy()) {
      logger.info("Waiting for all message sent ...")
      Thread.sleep(50)
    }
  }
}
