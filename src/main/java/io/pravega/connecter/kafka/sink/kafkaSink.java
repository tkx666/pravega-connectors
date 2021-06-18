package io.pravega.connecter.kafka.sink;

import io.pravega.connecter.runtime.sink.Sink;
import io.pravega.connecter.runtime.sink.SinkRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class kafkaSink implements Sink {

    private Producer<String, String> producer;
    private Map<String, String> kafkaProps;
    @Override
    public void open(Map<String, String> sinkProps, Map<String, String> pravegaProps) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers",sinkProps.get("bootstrap.servers"));
        properties.put("key.serializer",sinkProps.get("key.serializer"));
        properties.put("value.serializer",sinkProps.get("value.serializer"));
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
    }
}
class ProducerCallBack implements Callback {

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(exception != null){
            exception.printStackTrace();
        }
    }
}
