import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object Producer {
  def main(args: Array[String]): Unit = {
    writeToKafka("Orders")
  }

  def writeToKafka(topic: String): Unit = {

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](props)

    var count = 0
    while (true) {
      count += 1
      val record = new ProducerRecord[String, String](topic, s"key $count", s"value $count")
      val result = producer.send(record).get()
      println(result)
      producer.flush()
      Thread.sleep(1000)
    }
    producer.close()
  }
}
