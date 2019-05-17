package kafka.consumers

import com.typesafe.config.Config

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import java.util.Properties
import java.util.Arrays

class OrderConsumer(conf : Config) extends BaseConsumer(conf,"kafka.order") {
 	
 	{
    val sparkConf = new SparkConf()
      .setAppName("OrderStreamingFromRDD")
      .setMaster("local[*]")

    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.mapping.id", "id")
    val sc = new SparkContext(sparkConf)

    // streams will produce data every second
    this.ssc = new StreamingContext(sc, Seconds(1))

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
      println("*** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        // let's see how many partitions the resulting RDD has -- notice that it has nothing
        // to do with the number of partitions in the RDD used to publish the data (4), nor
        // the number of partitions of the topic (which also happens to be four.)
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
      }
    })

    this.ssc.start()
 	}
}