package io.pravega.connector.kafka.sink;

import io.pravega.connector.runtime.sink.Sink;
import io.pravega.connector.runtime.sink.SinkRecord;
import org.apache.kafka.clients.producer.*;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class kafkaSink implements Sink {

    private Producer<String, String> producer;
    private Map<String, String> kafkaProps;
    public static final String SERVER_SERVERS_CONFIG = "bootstrap.servers";
    public static final String KEY_SERIALIZER_CONFIG = "key.serializer";
    public static final String VALUE_SERIALIZER_CONFIG = "value.serializer";
    public static final String TOPIC_CONFIG="topic";
    @Override
    public void open(Map<String, String> sinkProps, Map<String, String> pravegaProps) {
        Properties properties = new Properties();
        properties.put(SERVER_SERVERS_CONFIG,sinkProps.get(SERVER_SERVERS_CONFIG));
        properties.put(KEY_SERIALIZER_CONFIG,sinkProps.get(KEY_SERIALIZER_CONFIG));
        properties.put(VALUE_SERIALIZER_CONFIG,sinkProps.get(VALUE_SERIALIZER_CONFIG));
        producer = new KafkaProducer<String, String>(properties);
        this.kafkaProps=sinkProps;

    }

    @Override
    public void close() {
        producer.close();

    }

    @Override
    public void write(List<SinkRecord> recordList) {
        for(SinkRecord sinkRecord: recordList){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(kafkaProps.get("topic"), null, (String) sinkRecord.getValue());
            producer.send(producerRecord,new ProducerCallBack());
        }
        producer.flush();
    }
}
class ProducerCallBack implements Callback {

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            exception.printStackTrace();
        }
    }
}
