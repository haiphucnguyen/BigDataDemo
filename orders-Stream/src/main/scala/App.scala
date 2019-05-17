import java.time
import java.time.{LocalDate, LocalDateTime}

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
          RandomStringUtils.randomAlphabetic(10),
          RandomStringUtils.randomAlphabetic(10),
          RandomStringUtils.randomAlphabetic(10),
          RandomUtils.nextDouble(),
          RandomUtils.nextInt(1, 1000)
        );
        items += item;
      }


      val cart = new Cart(
        RandomStringUtils.randomAlphabetic(10),
        RandomStringUtils.randomAlphabetic(10),
        LocalDateTime.now(),
        LocalDate.now().plusDays(3),
        new Address(RandomStringUtils.randomAlphabetic(10),
                    RandomStringUtils.randomAlphabetic(10),
                    RandomStringUtils.randomAlphabetic(10),
                    RandomStringUtils.randomAlphabetic(10)),
        items.toList
      );
      streamer.sendCart(cart);


      items.foreach(i => {
          val shipping = new ShippingStatus(
            RandomStringUtils.randomAlphabetic(5),
            "New",
            LocalDateTime.now(),
            new Address(RandomStringUtils.randomAlphabetic(10),
              RandomStringUtils.randomAlphabetic(10),
              RandomStringUtils.randomAlphabetic(10),
              RandomStringUtils.randomAlphabetic(10))
          );

          streamer.sendShippingStatus(shipping);
      });
    }
  }
}
