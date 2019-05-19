import com.typesafe.config.ConfigFactory
import net.liftweb.json.{DefaultFormats, NoTypeHints, Serialization, ShortTypeHints}
import net.liftweb.json.Serialization.{read, write}
import org.slf4j.{Logger, LoggerFactory}
import repositories.CartDB
import com.mekong.dto._
import net.liftweb.json.ext.{JodaTimeSerializers, JsonBoxSerializer}
import org.joda.time._
import stream.Streamer

object TestJsonApp {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(classOf[Streamer])
    val conf = ConfigFactory.load
    val timeRangeInput = conf.getLong("time-range")
    val timeRange = if (timeRangeInput<0) - timeRangeInput else timeRangeInput

    val cart = CartDB.randomCart(timeRange)

    implicit val formats = Serialization.formats(NoTypeHints) ++ JodaTimeSerializers.all + new JsonBoxSerializer
    val jsonStr = write(cart)

    logger.info("serialized {}", jsonStr)
    val cart2 = read[Cart](jsonStr)
    logger.info("deserialized {}", write(cart2))
    scala.io.StdIn.readLine("done")


    val x = JodaTypes(new Duration(10*1000), new Instant(System.currentTimeMillis),
      new DateTime, new DateMidnight, new Interval(1000, 50000),
      new LocalDate(2011, 1, 16), new LocalTime(16, 52, 10))
    val ser = write(x)
    read[JodaTypes](ser)
  }
}

case class JodaTypes(duration: Duration, instant: Instant, dateTime: DateTime,
                     dateMidnight: DateMidnight, interval: Interval, localDate: LocalDate,
                     localTime: LocalTime)

case class Dates(dt: DateTime, dm: DateMidnight)
