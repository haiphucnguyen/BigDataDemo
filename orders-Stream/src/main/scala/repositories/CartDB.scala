package repositories

import java.util.UUID
import org.joda.time._
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}
import scala.collection.mutable.ArrayBuffer

object CartDB {
  import com.mekong.dto._
  def randomCart(timeRange:Long): Cart ={
    val items=new ArrayBuffer[Order]()
    for(i <-0 to RandomUtils.nextInt(2, 6)) {
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


    val carttime = Instant.now().minus(timeRange)
    new Cart(
      Id[Cart](UUID.randomUUID().toString),
      RandomStringUtils.randomAlphabetic(10),
      carttime,
      carttime.plus(3),
      items.toList
    )
  }
}
