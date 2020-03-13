package kafkaDemo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;



public class ConsumerDemo {

	public static void main(String[] args) {
		//1、创建kafka生产者的配置信息，ProducerConfig类里面有所有的配置信息
		Properties properties = new Properties();
		//2、指定连接的集群
		properties.put("bootstrap.servers", "smaster:9092");
		//3、重试次数
		properties.put(ProducerConfig.RETRIES_CONFIG, 1);
		//4、批次大小，16k，当一个batch达到16k就会写到buffer里面
		properties.put("batch.size", 16384);
		//5、等待时间，如果在1ms内一个batch没有达到16k，也写到buffer里面
		properties.put("linger.ms",1);
		//6、RecordAccumulator缓冲区大小，32M
		properties.put("buffer.memory",33554432);
		//8、ack级别
		properties.put("acks", "all");
		//7、序列化
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		
		for (int i = 100; i < 110; i++)
		     producer.send(new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i)));
		//关闭资源，这个很重要。
		producer.close();
	}

}
