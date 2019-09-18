package org.bd.cart

import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}

import scala.collection.mutable

object ZipCodeDB {
  val STATES = Array("Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode", "Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")

  val CITIES = Map(
    "California" -> List("Los Angeles", "San Francisco", "Orange"),
    "Texas" -> List("Dallas", "Austin", "Houston"),
    "Iowa" -> List("Iowa", "Des Moines", "Fairfield", "Ottumwa"),
    "New York" -> List("New York", "Albany", "Columbia"),
    "Illinois" -> List("Chicago", "Decatur"),
    "Florida" -> List("Miami", "Orlando")
  )

  val citiStates = new mutable.ListBuffer[(Integer, String, String)]
  var totalP = 0
  CITIES.foreach(s => {
    s._2.foreach(c => {
      val p = RandomUtils.nextInt(1, 100000)
      totalP += p
      citiStates += Tuple3(p, c, s._1)
    })
  })

  def nextRandomAddress(): Map[String, String] = {
    val p = RandomUtils.nextInt(0, totalP)
    var acc = 0
    for (idx <- 0 to citiStates.size - 1) {
      acc += citiStates(idx)._1
      if (acc > p) {
        return Map(
          "address" -> RandomStringUtils.randomAlphabetic(10),
          "city" -> citiStates(idx)._2,
          "zip" -> RandomStringUtils.randomNumeric(5),
          "state" -> citiStates(idx)._3
        )
      }
    }

    val idx = RandomUtils.nextInt(0, citiStates.size)
    Map(
      "address" -> RandomStringUtils.randomAlphabetic(10),
      "city" -> citiStates(idx)._2,
      "zip" -> RandomStringUtils.randomNumeric(5),
      "state" -> citiStates(idx)._3
    )
  }
}
