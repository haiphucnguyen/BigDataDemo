package stream;

import com.google.gson.Gson;
import com.mekong.dto.Cart;
import com.mekong.dto.ShippingAddress;
import com.mekong.dto.ShippingStatus;
import com.typesafe.config.Config;
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

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Streamer {

    private String server;
    private String orderTopic;
    private String shippingTopic;
    private String shipingStatusTopic;
    private Producer<String, String> producer;

    public Streamer(Config conf) {
        server = conf.getString("kafka.server");
        orderTopic = conf.getString("kafka.order");
        shippingTopic = conf.getString("kafka.shipping");
        shipingStatusTopic = conf.getString("kafka.shippingstatus");
        this.ensureTopic(orderTopic);
        this.ensureTopic(shippingTopic);
        this.ensureTopic(shipingStatusTopic);
        this.createProducers();
        logger.info("streamer {} {} {} {}", this.server, this.orderTopic, this.shippingTopic, this.shipingStatusTopic);
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

    private boolean sendData(String topic, String id, String data) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, id, data);
        try {
            logger.error("Sending message {} {}", topic, id);

            this.producer.send(record).get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            // TODO Auto-generated catch block
            logger.error("Sending message error {}", e.getMessage());
            return false;
        }
    }

    public boolean sendCart(Cart cart) {
        logger.info("Sending order message");
        return this.sendData(this.orderTopic, cart.cardId().toString(), gson.toJson(cart));
    }

    public boolean sendShipping(ShippingAddress shipping) {
        logger.info("Sending shipping message");
        return this.sendData(this.shippingTopic, shipping.cartId().toString(), gson.toJson(shipping));
    }

    public boolean sendShippingStatus(ShippingStatus status) {
        logger.info("Sending shipping status message");
        String data = gson.toJson(status);
        return this.sendData(this.shipingStatusTopic, "" + status.orderId(), data);
    }
}
