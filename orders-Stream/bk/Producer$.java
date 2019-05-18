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
