package org.bd.cart

import java.util
import java.util.Properties
import java.util.concurrent.{ExecutionException, Future}

import com.typesafe.config.Config
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, CreateTopicsResult, NewTopic}
import org.apache.kafka.clients.producer._
import org.slf4j.LoggerFactory

class Streamer(config: Config) {
  private val logger = LoggerFactory.getLogger(classOf[Streamer])

  private val server = config.getString("kafka.server");
  private val cartTopic = config.getString("kafka.cart");
  private val shippingTopic = config.getString("kafka.shipping");
  private val shippingStatusTopic = config.getString("kafka.shippingstatus");

  val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  private val producer = new KafkaProducer[String, String](props)
  private var sendingProceses = 0
  this.ensureTopic(cartTopic)
  this.ensureTopic(shippingStatusTopic)

  private def ensureTopic(topic: String): Unit = {
    val props: Properties = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    val adminClient: AdminClient = AdminClient.create(props)
    // by default, create topic with 1 partition, use Kafka tools to change this topic to scale.
    val cTopic: NewTopic = new NewTopic(topic, 1, 1.toShort)
    val createTopicsResult: CreateTopicsResult = adminClient.createTopics(util.Arrays.asList(cTopic))
    try createTopicsResult.all.get
    catch {
      case e@(_: InterruptedException | _: ExecutionException) =>
        logger.error("Create topic error {}", e.getMessage)
    }
  }

  def sendCart(cart: String, id: String): Future[RecordMetadata] = this.sendData(this.cartTopic, id, cart)

  def sendShipping(shipping: String, id: String): Future[RecordMetadata] = this.sendData(this.shippingTopic, id, shipping)

  def isOverload(): Boolean = sendingProceses > 100

  def isBusy(): Boolean = sendingProceses > 0

  private def sendData(topic: String, id: String, data: String): Future[RecordMetadata] = {
    this.sendingProceses += 1
    logger.info("Sending $topic $data")
    val record = new ProducerRecord[String, String](topic, id, data)
    producer.send(record, new Callback {
      def onCompletion(var1: RecordMetadata, var2: Exception) = {
        sendingProceses -= 1
      }
    })
  }
}
