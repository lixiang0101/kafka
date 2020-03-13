package kafkaDemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put("metadata.broker.list","smaster:9092");
		props.put("bootstrap.servers", "smaster:9092");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		for (int i = 0; i < 100; i++) {
			Thread.sleep(5000);
			producer.send(new ProducerRecord<String, String>("kafkaTest01",
					"这是kafkaTest01 topic的第" + i + "消息"));
		}
		producer.close();
	}

}
