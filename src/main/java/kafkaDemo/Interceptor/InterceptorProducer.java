package kafkaDemo.Interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 *
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "smaster:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"kafkaDemo.MyPartitioner");

        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("kafkaDemo.Interceptor.TimeInterceptor");
        interceptors.add("kafkaDemo.Interceptor.CounterInterceptor");

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<>("third", Integer.toString(i), Integer.toString(i)),(metadata, exception) -> {
                if ( exception == null ) {//如果没有返回异常，说明发送成功了
                    System.out.println("Success ->" + metadata.partition() + "-->" + metadata.offset());
                }else {
                    exception.printStackTrace();;
                }
            });
        producer.close();
    }

}
