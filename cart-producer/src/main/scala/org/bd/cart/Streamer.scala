package org.bd.cart

import java.util
import java.util.Properties
import java.util.concurrent.{ExecutionException, Future}

import com.typesafe.config.Config
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
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
  private var sendingProcesses = 0
  this.ensureTopic(cartTopic)
  this.ensureTopic(shippingStatusTopic)

  private def ensureTopic(topic: String): Unit = {
    val props: Properties = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    val adminClient = AdminClient.create(props)
    // by default, create topic with 1 partition, use Kafka tools to change this topic to scale.
    val cTopic = new NewTopic(topic, 1, 1.toShort)
    val createTopicsResult = adminClient.createTopics(util.Arrays.asList(cTopic))
    try createTopicsResult.all.get
    catch {
      case e@(_: InterruptedException | _: ExecutionException) =>
        logger.error("Create topic error {}", e.getMessage)
    }
  }

  def sendCart(cart: String, id: String): Future[RecordMetadata] = sendData(cartTopic, id, cart)

  def sendShipping(shipping: String, id: String): Future[RecordMetadata] = sendData(shippingTopic, id, shipping)

  def isOverload(): Boolean = sendingProcesses > 100

  def isBusy(): Boolean = sendingProcesses > 0

  private def sendData(topic: String, id: String, data: String): Future[RecordMetadata] = {
    this.sendingProcesses += 1
    logger.info(s"Sending $topic $data")
    val record = new ProducerRecord[String, String](topic, id, data)
    producer.send(record, new Callback {
      def onCompletion(var1: RecordMetadata, var2: Exception) = {
        sendingProcesses -= 1
      }
    })
  }
}
