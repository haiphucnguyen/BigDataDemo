package kafka.consumers

import com.typesafe.config.Config

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import java.util.Properties
import java.util.Arrays


abstract class BaseConsumer (conf:Config, topicConfig:String){
  protected var _server : String = ""
  protected var _group : String  = ""
  protected var _topic : String = ""


  def getBasicStringStringConsumer() : Properties = {
    val consumerConfig: Properties = new Properties
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this._server)
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, this._group)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    //consumerConfig.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY, "roundrobin")

    consumerConfig
  }
 	
  
  {
    this._server = conf.getString("kafka.server")
		this._group = conf.getString("kafka.group")
		this._topic = conf.getString(topicConfig)		
  }
}