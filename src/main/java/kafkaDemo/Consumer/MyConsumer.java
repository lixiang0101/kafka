package kafkaDemo.Consumer;

import org.apache.kafka.clients.consumer.*;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 *
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"smaster:9092");
        //设置是否自动提交offest
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        //设置自动提交的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        //设置组id
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flumeConsum");
        //反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //设置订阅的会话，一个消费者可以订阅多个会话
        consumer.subscribe(Arrays.asList("first","second"));
        while (true){//保持线程阻塞，随时都可以接受消息
            //poll返回的是批量的Records，需要进行遍历解析
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                //System.out.println(record.value());
                System.out.printf("topic = %s,offset = %d, key = %s, value = %s%n",record.topic(), record.offset(), record.key(), record.value());
            }
        }
    }
}
