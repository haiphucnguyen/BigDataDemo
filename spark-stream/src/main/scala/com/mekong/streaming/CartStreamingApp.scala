package com.mekong.streaming

import com.fasterxml.jackson.databind.{JsonMappingException, ObjectMapper}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mekong.dto.Cart
import com.mekong.streaming.CartStreamingApp.jsonMapper
import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CartStreamingApp {
  val product = s"""{
               |"table":{"namespace":"default", "name":"shcExampleTable", "tableCoder":"PrimitiveType"},
               |"rowkey":"key",
               |"columns":{
               |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
               |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
               |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
               |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
               |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
               |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
               |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
               |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
               |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
               |}
               |}""".stripMargin

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("Cart Streaming Processing")
      .config("spark.driver.allowMultipleContexts", "true")
      .master("local[*]")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,localhost:9093,kafka:9092,kafka:9093",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "OrdersStream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val cartStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array("cart-topic"), kafkaParams)
    )

    val shippingStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array("shipping-topic"), kafkaParams)
    )

    cartStream.foreachRDD(rdd => {
      rdd.foreachPartition { iter =>
        iter.map(cr => jsonMapper().readValue(cr.value(), classOf[Cart]))
      }
    }
    )

    //    shippingStream.foreachRDD(rdd => {
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      rdd.foreachPartition { iter =>
    //        iter.foreach(cr => println(cr.key() + " " + cr.value()))
    //      }
    //    }
    //    )

    ssc.start()
    ssc.awaitTermination()

    ssc.stop()
  }

  def jsonMapper(): ObjectMapper = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModules(DefaultScalaModule, new JavaTimeModule())
    mapper
  }
}