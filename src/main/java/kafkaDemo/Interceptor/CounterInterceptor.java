package kafkaDemo.Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 *  这个拦截器的作用是统计消息发送成功和失败的次数
 */
public class CounterInterceptor implements ProducerInterceptor<String,String> {
    int success;
    int error;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        //如果返回异常说明消息没有发送成功，这个方法会在每发送一条消息就会调用一次
        if (exception != null){
            error ++;
        }else {
            success ++;
        }
    }

    @Override
    public void close() {
        //在所有的消费都发送完之后再打印出成功和失败的数量
        System.out.println("success: " + success);
        System.out.println("error: " + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
