import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;

import com.mekong.dto.Address;
import com.mekong.dto.Cart;
import com.mekong.dto.Order;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import scala.Function1;
import scala.Function2;
import scala.PartialFunction;
import scala.Tuple2;
import scala.collection.*;
import scala.collection.Iterable;
import scala.collection.generic.CanBuildFrom;
import scala.collection.immutable.List;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Traversable;
import stream.Streamer;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Config conf = ConfigFactory.load();
		
		
		Streamer streamer = new Streamer(conf);
		for(int loop=0;loop<100;loop++) {
			Cart cart = new Cart(
					RandomStringUtils.randomAlphabetic(10),
					RandomStringUtils.randomAlphabetic(10),
					LocalDateTime.now(),
					LocalDate.now().plusDays(3),
					new Address(RandomStringUtils.randomAlphabetic(10),
							RandomStringUtils.randomAlphabetic(10),
							RandomStringUtils.randomAlphabetic(10),
							RandomStringUtils.randomAlphabetic(10)),
					JavaConversions.asScalaBuffer(new ArrayList<Order>()).toList()
			);
			for(int i=0;i<5;i++) {
				Order item = new Order(
						RandomStringUtils.randomAlphabetic(10),
						RandomStringUtils.randomAlphabetic(10),
						RandomStringUtils.randomAlphabetic(10),
						RandomUtils.nextDouble(),
								RandomUtils.nextInt(1, 1000)

				);
				cart.orders().$plus$colon(item);
			}
			streamer.sendOrder(order);
			
			
			Shipping shipping = new Shipping();
			shipping.setNumber(order.getNumber());
			shipping.setUser(order.getUser());
			shipping.setExpectedDate(new Date());
			shipping.setCity(RandomStringUtils.randomAlphabetic(5));
			shipping.setState(RandomStringUtils.randomAlphabetic(2));
			shipping.setZipcode(RandomStringUtils.randomAlphabetic(5));
			
			for(int i=0;i<5;i++) {
				order.getItems().add(order.getItems().get(i));
			}
			streamer.sendShipping(shipping);		
			
			
			for(int i=0;i<2;i++) {
				ShippingStatus status = new ShippingStatus();
				status.setId(RandomUtils.nextInt());
				status.setNumber(shipping.getNumber());
				status.setDate(new Date());
				status.setStatus(RandomStringUtils.randomAlphabetic(5));
				streamer.sendShippingStatus(status);	
			}
		}
	}

}
