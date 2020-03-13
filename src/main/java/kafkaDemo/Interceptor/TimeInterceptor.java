package kafkaDemo.Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 *  自定义一个拦截器，在消息发送钱，在value前加上时间戳
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String value = record.value();
        //重新创建一个ProducerRecord对象并返回
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(record.topic(), record.partition(),
                record.key(), System.currentTimeMillis() + "," + value);
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
