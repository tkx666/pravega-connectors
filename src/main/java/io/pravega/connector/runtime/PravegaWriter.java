package io.pravega.connector.runtime;

import com.sun.jndi.dns.ResourceRecord;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.*;
import io.pravega.connector.runtime.source.SourceRecord;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PravegaWriter implements Writer {
    private EventStreamWriter<Object> writer;
    private Map<String, String> pravegaProps;
    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String ROUTING_KEY_CLASS_CONFIG = "routingKey.class";

    public PravegaWriter(Map<String, String> pravegaProps) throws IllegalAccessException, InstantiationException {
        this.pravegaProps = pravegaProps;
    }

    public boolean initialize() {
        try {
            Class serializerClass = Class.forName(pravegaProps.get(SERIALIZER_CONFIG));
            String scope = pravegaProps.get(SCOPE_CONFIG);
            URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
            String streamName = pravegaProps.get(STREAM_NAME_CONFIG);
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                    ClientConfig.builder().controllerURI(controllerURI).build());
            this.writer = clientFactory.createEventWriter(streamName,
                    (Serializer) serializerClass.newInstance(),
                    EventWriterConfig.builder().build());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return true;
    }

    public void write(List<SourceRecord> records) {
        try {
            RoutingKeyGenerator generator = null;
            if (pravegaProps.containsKey(ROUTING_KEY_CLASS_CONFIG)) {
                Class routingKeyGeneratorClass = Class.forName(pravegaProps.get(ROUTING_KEY_CLASS_CONFIG));
                generator = (RoutingKeyGenerator) routingKeyGeneratorClass.newInstance();
            }
            for(SourceRecord record: records) {
                Object message = record.getValue();
                if (pravegaProps.containsKey(ROUTING_KEY_CLASS_CONFIG)) {
                    String routingKey = generator.generateRoutingKey(message);
                    final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
                } else {
                    final CompletableFuture writeFuture = writer.writeEvent(message);
                }
                writer.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        writer.close();

    }

}
