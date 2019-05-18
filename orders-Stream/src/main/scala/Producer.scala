import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.{Properties, UUID}

import com.goyeau.kafka.streams.circe.CirceSerdes
import com.mekong.dto.{Cart, Id}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

object Producer {
  def main(args: Array[String]): Unit = {
    writeToKafka("Carts")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    val producer = new KafkaProducer(props,
      CirceSerdes.serializer[Id[Cart]],
      CirceSerdes.serializer[Cart])

    val cart = Cart(Id[Cart](UUID.randomUUID().toString), "user", Instant.now(),
      deliveredOn = Instant.now().plus(2, ChronoUnit.DAYS), null)
    //    var count = 0
    //    while (true) {
    //      count += 1
    //      val record = new ProducerRecord[Id[Cart], Cart](topic, null, null)
    //      val result = producer.send(record).get()
    //      println(result)
    //      producer.flush()
    //      Thread.sleep(1000)
    //    }
    //    producer.close()
  }
}
