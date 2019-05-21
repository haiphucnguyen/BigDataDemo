package com.mekong.streaming.hbase

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mekong.dto.Cart
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object CartStreamingHBaseBatchApp {
  val productSales =
    s"""{
       |"table":{"namespace":"default", "name":"productSalesTbl", "tableCoder":"PrimitiveType"},
       |"rowkey":"id",
       |"columns":{
       |"itemKey":{"cf":"rowkey", "col":"id", "type":"string"},
       |"productId":{"cf":"cf1", "col":"productId", "type":"string"},
       |"productCategory":{"cf":"cf2", "col":"productCategory", "type":"string"},
       |"totalAmount":{"cf":"cf3", "col":"totalAmount", "type":"double"},
       |"issueTimeStamp":{"cf":"cf4", "col":"issueTimeStamp", "type":"bigint"}
       |}
       |}""".stripMargin

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
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()

    val sqlCotext = spark.sqlContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(15))
    val topics = Array("cart-topic")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => record.value()).map(value => jsonMapper().readValue(value, classOf[Cart]))
      .map(cart => {
        val result = new ArrayBuffer[ProductSalesRecord]()
        val orders = cart.orders
        orders.foreach(order => {
          result += ProductSalesRecord(order.productId, order.productId, order.productCategory,
            order.amount * order.price, cart.issuedTimestamp)
        })
        result
      }).foreachRDD(rdd => {
      import sqlCotext.implicits._
      rdd.flatMap(item => item.toList).toDF().write.options(
        Map(HBaseTableCatalog.tableCatalog -> productSales, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
