import com.typesafe.config.{Config, ConfigFactory}
import kafka.consumers.{BaseConsumer, OrderConsumer, ShippingConsumer, ShippingStatusConsumer}

import scala.collection.mutable.ArrayBuffer

object App {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load

    val consumers:ArrayBuffer[BaseConsumer] = new ArrayBuffer[BaseConsumer]()
    consumers += new OrderConsumer(conf)
    consumers += new ShippingConsumer(conf)
    consumers += new ShippingStatusConsumer(conf)

    var input : String = ""
    do {
      input = scala.io.StdIn.readLine("[press q to quite]")
    }while (!input.equals("q"))

    consumers.foreach(c => {c.stop()})
  }
}