package com.mekong.streaming.hbase

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.mekong.dto.Cart
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{SparkSession, _}

import scala.collection.mutable.ArrayBuffer

object CartStreamingHBaseApp2 {
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

  def main(args: Array[String]): Unit = {

    def jsonMapper(): ObjectMapper = {
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModules(DefaultScalaModule, new JavaTimeModule())
      mapper
    }

    val spark = SparkSession.builder()
      .appName("Cart Streaming Processing")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9093")
      .option("startingOffsets", "earliest")
      .option("subscribe", "cart-topic")
      .load()

    import spark.implicits._
    val ssData = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].toDF()
      .map(row => row.getString(1)).map(value => jsonMapper().readValue(value, classOf[Cart]))
      .map(cart => {
        val result = new ArrayBuffer[ProductSalesRecord]()
        val orders = cart.orders
        orders.foreach(order => {
          result += ProductSalesRecord(order.productId, order.productId, order.productCategory,
            order.amount * order.price, cart.issuedTimestamp)
        })
        result
      }).writeStream
      //                  .foreachBatch((batchDF: Dataset[ArrayBuffer[ProductSalesRecord]], batchId: Long) => {
      //                    batchDF.flatMap(_.toList).write
      //                      .format("console")
      //                      .save()
      //                  }).start().awaitTermination()
      .foreachBatch((batchDF: Dataset[ArrayBuffer[ProductSalesRecord]], _: Long) => {
      batchDF.flatMap(_.toList).write.options(
        Map(HBaseTableCatalog.tableCatalog -> productSales, HBaseTableCatalog.newTable -> "5"))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }).start().awaitTermination()
  }
}
