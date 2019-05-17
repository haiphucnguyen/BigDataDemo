import com.mekong.dto.ShippingAddress

import scala.util.Random

object DB {

  val STATES = Array("Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode", "Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")


  val USERS = generateUsersList()

  def generateUsersList(): Map[String, ShippingAddress] = {
    var result = Map[String, ShippingAddress]()

    val random = new Random()
    for (i <- 0 until 10000) {
      val user = s"user-$i"
      val address = new ShippingAddress("address", "city", "", STATES(random.nextInt(50)))
      result += (user -> address)
    }
    result
  }
}
