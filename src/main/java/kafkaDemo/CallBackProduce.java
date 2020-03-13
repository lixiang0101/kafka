package kafkaDemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CallBackProduce {

	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "smaster:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"kafkaDemo.MyPartitioner");
		
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for (int i = 0; i < 10; i++)
		     producer.send(new ProducerRecord<>("third", Integer.toString(i), Integer.toString(i)),(metadata,exception) -> {
				if ( exception == null ) {//如果没有返回异常，说明发送成功了
					System.out.println("Success ->" + metadata.partition() + "-->" + metadata.offset());
				}else {
					exception.printStackTrace();;
				}
			});
		producer.close();
	}

}
