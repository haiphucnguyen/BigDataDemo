package com.mekong.streaming.consumer

import java.util.{Arrays, Properties}

import com.typesafe.config.Config
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.LoggerFactory

class ShippingConsumer (conf : Config,ssc : StreamingContext) extends BaseConsumer(conf,"kafka.shipping") {
  private val logger = LoggerFactory.getLogger(classOf[ShippingConsumer])

 	{
    // Create the stream.
    val props: Properties = this.getBasicStringStringConsumer()

    val kafkaStream =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](
          Arrays.asList(this._topic),
          props.asInstanceOf[java.util.Map[String, Object]]
        )

      )

    // now, whenever this Kafka stream produces data the resulting RDD will be printed
    kafkaStream.foreachRDD(r => {
      logger.info("*** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        logger.info("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => logger.info("*** partition size = " + a.size))
      }
    })

 	}
}