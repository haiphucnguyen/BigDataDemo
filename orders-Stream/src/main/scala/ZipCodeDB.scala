import java.io.File

import com.github.tototoshi.csv.CSVReader
import org.apache.commons.lang3.{RandomStringUtils, RandomUtils}


object ZipCodeDB {
  val reader = CSVReader.open(new File()("uszips.csv"))
  val zipCodes = reader.allWithHeaders()
  def nextRandomAddress() : Map[String,String] = {
    val res = zipCodes(RandomUtils.nextInt(0, zipCodes.size)).clone()
    res("address", RandomStringUtils.randomAlphabetic(10))
  }
}
