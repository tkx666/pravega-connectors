package io.pravega.connecter.kafka.source;

import io.pravega.connecter.runtime.source.Source;
import io.pravega.connecter.runtime.source.SourceRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class kafkaSource implements Source {
    private KafkaConsumer<String, String> consumer;
    public static final String SERVER_SERVERS_CONFIG = "bootstrap.servers";
    public static final String KEY_DESERIALIZER_CONFIG = "key.deserializer";
    public static final String VALUE_DESERIALIZER_CONFIG = "value.deserializer";
    public static final String TOPIC_CONFIG = "topic";
    public static final String GROUP_ID_CONFIG = "group.id";

    @Override
    public void open(Map<String, String> sourceProps, Map<String, String> pravegaProps) {
        Properties properties = new Properties();
        properties.put(SERVER_SERVERS_CONFIG, sourceProps.get(SERVER_SERVERS_CONFIG));
        properties.put(KEY_DESERIALIZER_CONFIG, sourceProps.get(KEY_DESERIALIZER_CONFIG));
        properties.put(VALUE_DESERIALIZER_CONFIG, sourceProps.get(VALUE_DESERIALIZER_CONFIG));
        properties.put(GROUP_ID_CONFIG, sourceProps.get(GROUP_ID_CONFIG));
        this.consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(sourceProps.get(TOPIC_CONFIG)));
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public List<SourceRecord> read() {
        List<SourceRecord> sourceList = new ArrayList<>();

        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(5000);
            if (consumerRecords.count() == 0) return sourceList;
            for (ConsumerRecord<String, String> record : consumerRecords) {
                sourceList.add(new SourceRecord(record.value()));
            }
        }

    }
}
