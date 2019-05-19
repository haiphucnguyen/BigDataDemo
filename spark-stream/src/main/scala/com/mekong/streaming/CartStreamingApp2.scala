package com.mekong.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mekong.dto.Cart
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{SparkSession, _}

import scala.collection.mutable.ArrayBuffer

object CartStreamingApp2 {
  val productSales =
    s"""{
       |"table":{"namespace":"default", "name":"productSalesTbl", "tableCoder":"PrimitiveType"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
       |"col1":{"cf":"cf1", "col":"proId", "type":"string"},
       |"col2":{"cf":"cf1", "col":"category", "type":"string"},
       |"col3":{"cf":"cf2", "col":"value", "type":"double"},
       |"col4":{"cf":"cf3", "col":"issueTimestamp", "type":"bigint"}
       |}
       |}""".stripMargin

  def main(args: Array[String]): Unit = {

    def jsonMapper(): ObjectMapper = {
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModules(DefaultScalaModule, new JavaTimeModule())
      mapper
    }

    val spark = SparkSession.builder()
      .appName("Cart Streaming Processing")
      .config("spark.driver.allowMultipleContexts", "true")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("bootstrap.servers", "kafka:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "cart-topic")
      .load()

    import spark.implicits._
    val ssData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].toDF()
      .map(row => row.getString(1)).map(value => jsonMapper().readValue(value, classOf[Cart]))
      .map(cart => {
        val result = new ArrayBuffer[(String, String, String, Double, Long)]()
        val orders = cart.orders
        orders.foreach(order => {
          result += new Tuple5[String, String, String, Double, Long](order.productId, order.productId, order.productCategory,
            order.amount * order.price, cart.issuedTimestamp)
        })
        result
      }).writeStream
      .foreachBatch((batchDF: Dataset[ArrayBuffer[(String, String, String, Double, Long)]], batchId: Long) => {
        batchDF.write
          .format("console")
          .save()
      }).start().awaitTermination()
//      .foreachBatch((batchDF: Dataset[ArrayBuffer[(String, String, String, Double, Long)]], batchId: Long) => {
//        batchDF.write.options(
//          Map(HBaseTableCatalog.tableCatalog -> productSales, HBaseTableCatalog.newTable -> "5"))
//          .format("org.apache.spark.sql.execution.datasources.hbase")
//          .save()
//      }).start().awaitTermination()
    //      .writeStream.format("console").start()
    //      .awaitTermination()
  }
}
