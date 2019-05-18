package stream;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.goyeau.kafka.streams.circe.CirceSerdes;
import com.mekong.dto.Cart;
import com.mekong.dto.Id;
import com.mekong.dto.ShippingAddress;
import com.mekong.dto.ShippingStatus;
import io.circe.Encoder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.typesafe.config.Config;

public class Streamer {
	
	private String server;
	private String orderTopic;
	private String shippingTopic;
	private String shipingStatusTopic;
	private Producer<String, String> producer;
	private int sendingProceses = 0;
	private Callback onDoneSendata = (RecordMetadata data, Exception exception) -> {
		this.sendingProceses --;
	};

	public int getSendingProceses(){
		return this.sendingProceses;
	}

	public Streamer(Config conf) {
		server = conf.getString("kafka.server");
		orderTopic = conf.getString("kafka.order");
		shippingTopic = conf.getString("kafka.shipping");
		shipingStatusTopic = conf.getString("kafka.shippingstatus");
		this.ensureTopic(orderTopic);
		this.ensureTopic(shipingStatusTopic);		
		this.createProducers();
		logger.info("streamer {} {}", this.server, this.orderTopic);
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
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		this.producer = new KafkaProducer<>(props);
	}
	
	private Future<RecordMetadata> sendData(String topic, String id, String data) {
		this.sendingProceses++;
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, data);
		return this.producer.send(record, this.onDoneSendata);
	}

	public Future<RecordMetadata> sendCart(Cart cart) {
		logger.info("Sending order message");
		return this.sendData(this.orderTopic, cart.cardId().toString(), gson.toJson(cart));
	}

	public Future<RecordMetadata> sendShipping(ShippingAddress shipping) {
		logger.info("Sending shipping message");
		return this.sendData(this.shippingTopic, shipping.cartId().toString(), gson.toJson(shipping));
	}

	public Future<RecordMetadata> sendShippingStatus(ShippingStatus status) {
		logger.info("Sending shipping status message");
		String data = gson.toJson(status);
		return this.sendData(this.shipingStatusTopic, "" + status.orderId(), data);
	}
}
