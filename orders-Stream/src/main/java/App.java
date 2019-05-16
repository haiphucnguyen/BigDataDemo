import java.util.Date;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import stream.Streamer;
import models.Order;
import models.OrderItem;
import models.Shipping;
import models.ShippingStatus;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Config conf = ConfigFactory.load();
		
		
		Streamer streamer = new Streamer(conf);
		for(int loop=0;loop<100;loop++) {
			Order order = new Order();
			order.setNumber(RandomStringUtils.random(10));
			order.setUser(RandomStringUtils.random(10));
			order.setDate(new Date());
			order.setCity(RandomStringUtils.random(5));
			order.setState(RandomStringUtils.random(2));
			order.setZipcode(RandomStringUtils.random(5));
			
			for(int i=0;i<5;i++) {
				OrderItem item = new OrderItem();
				item.setAmount(RandomUtils.nextInt(1,11));
				item.setPrice(RandomUtils.nextFloat((float)0.4999999, 1000));
				item.setProductid(RandomUtils.nextInt(1,1000));
				item.setProduct(RandomStringUtils.random(20));
				order.getItems().add(item);
			}
			streamer.sendOrder(order);
			
			
			Shipping shipping = new Shipping();
			shipping.setNumber(order.getNumber());
			shipping.setUser(order.getUser());
			shipping.setExpectedDate(new Date());
			shipping.setCity(RandomStringUtils.random(5));
			shipping.setState(RandomStringUtils.random(2));
			shipping.setZipcode(RandomStringUtils.random(5));
			
			for(int i=0;i<5;i++) {
				order.getItems().add(order.getItems().get(i));
			}
			streamer.sendShipping(shipping);		
			
			
			for(int i=0;i<2;i++) {
				ShippingStatus status = new ShippingStatus();
				status.setNumber(shipping.getNumber());
				status.setDate(new Date());
				status.setStatus(RandomStringUtils.random(5));
				streamer.sendShippingStatus(status);	
			}
		}
	}

}
