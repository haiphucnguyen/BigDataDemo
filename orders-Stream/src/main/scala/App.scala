import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import stream.Streamer

import net.liftweb.json._
import net.liftweb.json.Serialization.write
import repositories.{CartDB, ShippingDB}

object App {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(classOf[Streamer])
    val conf = ConfigFactory.load
    val streamer = new Streamer(conf)
    val timeRangeInput = conf.getInt("time-range")
    val timeRange = if (timeRangeInput<0) -timeRangeInput else timeRangeInput
    val delayTime = conf.getInt("message-delay")
    var shutdown = false
    implicit val formats = DefaultFormats
    new Thread(new Runnable {
      override def run(): Unit = {
        while(!shutdown) {
          val cart = CartDB.randomCart(timeRange)
          streamer.sendCart(write(cart), cart.cardId.toString)

          val shipping = ShippingDB.randomShipping(cart)
          streamer.sendShipping(write(shipping), shipping.cartId.toString)

          while(streamer.getSendingProceses() > 100){
            logger.info("To much message, waiting...")
            Thread.sleep(50)
          }

          Thread.sleep(delayTime)
        }
      }
    }).start()

    var input: String = ""
    do {
      input = scala.io.StdIn.readLine("[press q to quite]")
    } while (!input.equals("q"))
    shutdown = true
    while ( streamer.getSendingProceses > 0) {
      logger.info("Waiting for all message sent ...")
      Thread.sleep(50)
    }
  }
}
