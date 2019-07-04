package com.bd.streaming.hive

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mekong.dto.Cart
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object CartStreamingHiveApp {
  def jsonMapper(): ObjectMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModules(DefaultScalaModule, new JavaTimeModule())
    mapper
  }

  def main(args: Array[String]): Unit = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9093",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val spark = SparkSession.builder()
      .appName("Cart Streaming Processing")
      .master("local[*]")
      .config("spark.driver.allowMultipleContexts", "true")
      .config("hive.metastore.uris", "thrift://hive-metastore:9083")
      .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/hive")
      .enableHiveSupport()
      .getOrCreate()

    val sqlContext = spark.sqlContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(15))
    val topics = Array("cart-topic")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    sqlContext.sql("CREATE TABLE IF NOT EXISTS ProductSale (productId String, productCategory STRING, totalAmount DOUBLE, issueTimeStamp BIGINT) USING HIVE")

    stream.map(record => record.value()).map(value => jsonMapper().readValue(value, classOf[Cart]))
      .map(cart => {
        val result = new ArrayBuffer[ProductSalesRecord]()
        val orders = cart.orders
        orders.foreach(order => {
          result += ProductSalesRecord(order.productId, order.productCategory,
            order.amount * order.price, cart.issuedTimestamp)
        })
        result
      }).foreachRDD(rdd => {
      import sqlContext.implicits._
      rdd.flatMap(item => item.toList).toDF().write.mode(SaveMode.Append).format("hive")
        .saveAsTable("ProductSale")
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
