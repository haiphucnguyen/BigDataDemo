import java.time
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime}

import com.mekong.dto._
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}
import stream.Streamer

import scala.collection.mutable.ArrayBuffer

object App {
  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load
    val streamer = new Streamer(conf)

    for(loop <- 0 to 100) {

      val items=new ArrayBuffer[Order]();
      for(i <-0 to 5) {
        val item = new Order(
          Id[Order](RandomStringUtils.randomAlphabetic(10)),
          RandomStringUtils.randomAlphabetic(10),
          RandomStringUtils.randomAlphabetic(10),
          RandomUtils.nextDouble(),
          RandomUtils.nextInt(1, 1000)
        );
        items += item;
      }


      val cart = new Cart(
        Id[Cart](RandomStringUtils.randomAlphabetic(10)),
        RandomStringUtils.randomAlphabetic(10),
        Instant.now(),
        Instant.now().plus(3, ChronoUnit.DAYS),
        items.toList
      );
      streamer.sendCart(cart);


      items.foreach(i => {
          val shipping =
            new ShippingAddress(
              cart.cardId,
              RandomStringUtils.randomAlphabetic(10),
              RandomStringUtils.randomAlphabetic(10),
              RandomStringUtils.randomAlphabetic(10),
              RandomStringUtils.randomAlphabetic(10)
          );
          streamer.sendShipping(shipping);
      });
    }
  }
}
