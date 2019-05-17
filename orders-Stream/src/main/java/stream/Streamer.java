package stream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.typesafe.config.Config;

public class Streamer {
	
	private String server;
	private String orderTopic;
	private String shipingTopic;	
	private String shipingStatusTopic;
	
	private Producer<String, String> producer;
 
	
	public Streamer(Config conf) {
		server = conf.getString("kafka.server");
		orderTopic = conf.getString("kafka.order");
		shipingTopic = conf.getString("kafka.shipping");
		shipingStatusTopic = conf.getString("kafka.shippingstatus");
		this.ensureTopic(shipingTopic);
		this.ensureTopic(orderTopic);
		this.ensureTopic(shipingStatusTopic);		
		this.createProducers();
		logger.info("streamer {} {} {}", this.server, this.orderTopic, this.shipingTopic);
	}	
	
	private static final Logger logger = LoggerFactory.getLogger(Streamer.class);
	private static Gson gson = new Gson();
	
	private void ensureTopic(String topic) {
		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		AdminClient adminClient = AdminClient.create(props);

		// by default, create topic with 1 partition, use Kafka tools to change this topic to scale.
		NewTopic cTopic = new NewTopic(topic, 1, (short) 1);
		CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(cTopic));
		try {
			createTopicsResult.all().get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Create topic error {}", e.getMessage());
		}
	}
	
	
	private void createProducers() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		this.producer = new KafkaProducer<>(props);
	}
	
	private boolean sendData(String topic, String id, String data) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, data);
		try {
			this.producer.send(record).get();
			return true;
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			logger.error("Sending message error {}", e.getMessage());
			return false;
		}
	}
	
	public boolean sendOrder(models.Order order) {
		logger.info("Sending order message");
		String data = gson.toJson(order);
		return this.sendData(this.orderTopic, order.getNumber(), data);
	}	
	
	public boolean sendShipping(models.Shipping shipping) {
		logger.info("Sending shipping message");
		String data = gson.toJson(shipping);
		return this.sendData(this.shipingTopic, shipping.getNumber(), data);
	}
	
	public boolean sendShippingStatus(models.ShippingStatus status) {
		logger.info("Sending shipping status message");
		String data = gson.toJson(status);
		return this.sendData(this.shipingStatusTopic, "" + status.getId(), data);
	}
}
