import java.io.File

import com.github.tototoshi.csv.CSVReader
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}


object ZipCodeDB {
  //val reader = CSVReader.open(new File("./uszips.csv"))
  //val zipCodes = reader.allWithHeaders()

  val STATES = Array("Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode", "Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming")

  def nextRandomAddress() : Map[String,String] = {
    Map(
      "address"->RandomStringUtils.randomAlphabetic(10),
      "city" -> "city",
      "zip" -> RandomStringUtils.randomNumeric(5),
      "state" ->  STATES(RandomUtils.nextInt(0, STATES.size))
    )
  }
}
