package io.pravega.connecter.utils;

import io.pravega.connecter.runtime.sink.SinkRecord;
import org.apache.kafka.clients.producer.*;

import java.util.Map;
import java.util.Properties;

public class KafkaProducerUtil {
    private Producer<String, String> producer;
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);

        for(int i = 0; i < 50000; i++){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("sink", null, String.valueOf(i));
            producer.send(producerRecord);
        }
        producer.flush();
    }
}

class ProducerCallBackUtil implements Callback {

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            exception.printStackTrace();
        }
    }
}
