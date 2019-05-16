package com.mekong

import java.util.Date
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

object SparkWebLogs {

  def main(args: Array[String]): Unit = {
    var fromDate: Date = null
    var toDate: Date = null
    println("Enter date range (format: dd/MM/yyyy")
    println("From:")
    while (fromDate == null) {
      val dateStr = scala.io.StdIn.readLine()
      try {
        fromDate = formatDate2.parse(dateStr)
      } catch {
        case _: Exception => println("Invalid format. Please use dd/MM/yyyy")
      }
    }

    println("To:")
    while (toDate == null) {
      val dateStr = scala.io.StdIn.readLine()
      try {
        toDate = formatDate2.parse(dateStr)
      } catch {
        case _: Exception => println("Invalid format. Please use dd/MM/yyyy")
      }
    }

    val conf = new SparkConf().setAppName("Bonus App").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val accessLogs = lines.map(parseValue(_)).filter(_ != null).cache()

    // Question c
    val count401 = accessLogs.filter(_._2.before(toDate)).filter(_._2.after(fromDate)).filter(_._3 == "401").count()
    println(s"Number of 401 error from $fromDate to $toDate is $count401")

    // Question d
    val ipsAccessMoreThan20Times = accessLogs.map(logItem => Tuple2(logItem._1, 1)).reduceByKey((count1, count2) => count1 + count2).filter(_._2 > 20)
    println("All ips access the server more than 20 times")
    ipsAccessMoreThan20Times.foreach(println(_))
  }

  val formatDate1 = new java.text.SimpleDateFormat("dd/MMM/yyyy")
  val formatDate2 = new java.text.SimpleDateFormat("dd/MM/yyyy")
  val patternStr = "^([\\S.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)"
  val pattern = Pattern.compile(patternStr)

  def parseValue(line: String): (String, Date, String) = {
    try {
      val matcher = pattern.matcher(line)
      if (!matcher.matches()) {
        return null
      }
      val ip = matcher.group(1)
      val date = formatDate1.parse(matcher.group(4))
      val httpCode = matcher.group(6)
      (ip, date, httpCode)
    } catch {
      case _: Exception => null
    }
  }
}
