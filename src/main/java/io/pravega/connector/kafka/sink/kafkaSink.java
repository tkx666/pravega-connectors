package io.pravega.connector.kafka.sink;

import io.pravega.connector.runtime.Config;
import io.pravega.connector.runtime.sink.Sink;
import io.pravega.connector.runtime.sink.SinkRecord;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class kafkaSink implements Sink {
    private static final Logger logger = LoggerFactory.getLogger(kafkaSink.class);
    private Producer<String, String> producer;
    private Map<String, String> kafkaProps;
    public static final String SERVER_SERVERS_CONFIG = "bootstrap.servers";
    public static final String KEY_SERIALIZER_CONFIG = "key.serializer";
    public static final String VALUE_SERIALIZER_CONFIG = "value.serializer";
    public static final String TOPIC_CONFIG = "topic";

    private static final Config config = new Config().add(SERVER_SERVERS_CONFIG, Config.Type.STRING, "localhost:9092", new Config.NonEmptyStringValidator())
            .add(KEY_SERIALIZER_CONFIG, Config.Type.STRING, "org.apache.kafka.common.serialization.StringSerializer", new Config.NonEmptyStringValidator())
            .add(VALUE_SERIALIZER_CONFIG, Config.Type.STRING, "org.apache.kafka.common.serialization.StringSerializer", new Config.NonEmptyStringValidator())
            .add(TOPIC_CONFIG, Config.Type.STRING, null, null);

    @Override
    public Config config() {
        return config;
    }

    @Override
    public void open(Map<String, String> sinkProps) {
        Properties properties = new Properties();
        properties.put(SERVER_SERVERS_CONFIG, sinkProps.get(SERVER_SERVERS_CONFIG));
        properties.put(KEY_SERIALIZER_CONFIG, sinkProps.get(KEY_SERIALIZER_CONFIG));
        properties.put(VALUE_SERIALIZER_CONFIG, sinkProps.get(VALUE_SERIALIZER_CONFIG));
        producer = new KafkaProducer<String, String>(properties);
        this.kafkaProps = sinkProps;

    }

    @Override
    public void close() {
        producer.close();

    }

    @Override
    public void write(List<SinkRecord> recordList) {
        for (SinkRecord sinkRecord : recordList) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(kafkaProps.get("topic"), null, (String) sinkRecord.getValue());
            producer.send(producerRecord, new ProducerCallBack());
        }
        producer.flush();
    }
}

class ProducerCallBack implements Callback {

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            exception.printStackTrace();
        }
    }
}
