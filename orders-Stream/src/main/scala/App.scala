import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import com.mekong.dto._
import com.typesafe.config.ConfigFactory
import net.liftweb.json.Serialization.write
import net.liftweb.json._
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}
import org.slf4j.{Logger, LoggerFactory}
import stream.Streamer

import scala.collection.mutable.ArrayBuffer

object App {
  def main(args: Array[String]): Unit = {
    val logger: Logger =
      LoggerFactory.getLogger(classOf[Streamer])

    val conf = ConfigFactory.load
    val streamer = new Streamer(conf)
    val timeRangeInput = conf.getInt("time-range")
    val timeRange = if (timeRangeInput < 0) -timeRangeInput else timeRangeInput
    val delayTime = conf.getInt("message-delay")
    var shutdown = false
    implicit val formats = DefaultFormats
    new Thread(new Runnable {
      override def run(): Unit = {
        while (!shutdown) {
          val items = new ArrayBuffer[Order]();
          for (_ <- 0 to RandomUtils.nextInt(2, 6)) {
            val product = ProductDB.nextRandom()
            val item = new Order(
              Id[Order](UUID.randomUUID().toString),
              product("id"),
              product("category"),
              product("price").toDouble,
              RandomUtils.nextInt(1, 11)
            )
            items += item
          }

          val cartTime = Instant.now().plus(-timeRange, ChronoUnit.MINUTES)
          val cart = new Cart(
            Id[Cart](UUID.randomUUID().toString),
            RandomStringUtils.randomAlphabetic(10),
            cartTime,
            cartTime.plus(3, ChronoUnit.DAYS),
            items.toList
          )
          streamer.sendCart(write(cart), cart.cardId.toString)

          val address = ZipCodeDB.nextRandomAddress()
          val shipping =
            ShippingAddress(
              cart.cardId,
              address("address"),
              "city",
              address("zip"),
              address("state")
            )
          streamer.sendShipping(write(shipping), shipping.cartId.toString)

          while (streamer.getSendingProceses() > 100) {
            logger.info("To much message, waiting...")
            Thread.sleep(50)
          }

          Thread.sleep(delayTime)
        }
      }
    }).start()

    val input = scala.io.StdIn.readLine()
    shutdown = true
    while (streamer.getSendingProceses > 0) {
      logger.info("Waiting for all message sent ...")
      Thread.sleep(50)
    }
  }
}
