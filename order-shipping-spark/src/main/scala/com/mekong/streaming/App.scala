package com.mekong.streaming

import com.mekong.streaming.consumer.{BaseConsumer, OrderConsumer, ShippingConsumer, ShippingStatusConsumer}
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object App {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val sparkConf = new SparkConf()
      .setAppName("OrderStreamingFromRDD")
      .setMaster("local[*]")

    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.mapping.id", "id")
    val sc = new SparkContext(sparkConf)

    // streams will produce data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    val consumers: ArrayBuffer[BaseConsumer] = new ArrayBuffer[BaseConsumer]()
    consumers += new OrderConsumer(conf, ssc)
    consumers += new ShippingConsumer(conf, ssc)
    consumers += new ShippingStatusConsumer(conf, ssc)

    ssc.start()
    val input = scala.io.StdIn.readLine()

    ssc.stop()
  }
}