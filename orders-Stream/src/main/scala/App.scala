import java.time
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime}
import java.util.UUID
import java.util.concurrent.Future

import com.mekong.dto._
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.{Logger, LoggerFactory}
import stream.Streamer

import scala.collection.mutable.ArrayBuffer

object App {
  def main(args: Array[String]): Unit = {
    val logger: Logger =
    LoggerFactory.getLogger(classOf[Streamer])

    val conf = ConfigFactory.load
    val streamer = new Streamer(conf)
    val timeRange = if (args.size > 0) args(0).toInt else 10
    val numOfCarts = if (args.size > 1) args(1).toInt else 100
    for(loop <- 0 to numOfCarts) {
      val items=new ArrayBuffer[Order]();
      for(i <-0 to RandomUtils.nextInt(2, 6)) {
        val product = ProductDB.nextRandom()
        val item = new Order(
          Id[Order](UUID.randomUUID().toString),
          product("Id"),
          product("category"),
          product("price").toDouble,
          RandomUtils.nextInt(1, 11)
        );
        items += item;
      }


      val carttime = Instant.now().plus(timeRange, ChronoUnit.MINUTES)
      val cart = new Cart(
        Id[Cart](UUID.randomUUID().toString),
        RandomStringUtils.randomAlphabetic(10),
        carttime,
        carttime.plus(3, ChronoUnit.DAYS),
        items.toList
      );
      streamer.sendCart(cart);
      items.foreach(i => {
        var address =ZipCodeDB.nextRandomAddress()
          val shipping =
            new ShippingAddress(
              cart.cardId,
              address("address"),
              address("city"),
              address("zip"),
              address("state_id")
          );
          streamer.sendShipping(shipping);
      });

      while(streamer.getSendingProceses() > 200){
        logger.info("To much message, waiting...")
        Thread.sleep(50);
      }
    }

    while ( streamer.getSendingProceses > 0) {
      logger.info("Waiting for all message sent ...")
      Thread.sleep(50)
    }
  }
}
