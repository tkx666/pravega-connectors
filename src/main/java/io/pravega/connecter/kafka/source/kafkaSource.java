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
    private KafkaConsumer<String,String> consumer;
    @Override
    public void open(Map<String, String> sourceProps, Map<String, String> pravegaProps) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers",sourceProps.get("bootstrap.servers"));
        properties.put("key.deserializer",sourceProps.get("key.deserializer"));
        properties.put("value.deserializer",sourceProps.get("value.deserializer"));
        properties.put("group.id",sourceProps.get("group.id"));
        this.consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(sourceProps.get("topic")));
    }

    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public List<SourceRecord> read() {
        List<SourceRecord> sourceList = new ArrayList<>();
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
                if (consumerRecords.count() == 0) return sourceList;
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    sourceList.add(new SourceRecord(record.value()));
                }
            }
        } finally {
            close();
        }
    }
}
