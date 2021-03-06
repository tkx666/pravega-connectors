package io.pravega.connector.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.connector.runtime.configs.WorkerConfig;
import io.pravega.connector.runtime.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * PravegaWriter encapsulates the EventStreamWriter.
 */
public class PravegaWriter implements Writer {
    private static final Logger logger = LoggerFactory.getLogger(PravegaWriter.class);
    private EventStreamWriter<Object> writer;
    private Map<String, String> pravegaProps;
    private EventStreamClientFactory clientFactory;

    public PravegaWriter(Map<String, String> pravegaProps) throws IllegalAccessException, InstantiationException {
        this.pravegaProps = pravegaProps;
    }

    public boolean initialize() {
        try {
            Class<?> serializerClass = Class.forName(pravegaProps.get(WorkerConfig.SERIALIZER_CONFIG));
            String scope = pravegaProps.get(WorkerConfig.SCOPE_CONFIG);
            URI controllerURI = URI.create(pravegaProps.get(WorkerConfig.URI_CONFIG));
            String streamName = pravegaProps.get(WorkerConfig.STREAM_NAME_CONFIG);
            clientFactory = EventStreamClientFactory.withScope(scope,
                    ClientConfig.builder().controllerURI(controllerURI).build());
            this.writer = clientFactory.createEventWriter(streamName,
                    (Serializer) serializerClass.newInstance(),
                    EventWriterConfig.builder().build());
        } catch (Exception e) {
            logger.error("writer initialize error", e);
        }

        return true;
    }

    public void write(List<SourceRecord> records) {
        try {
            RoutingKeyGenerator generator = null;
            if (pravegaProps.containsKey(WorkerConfig.ROUTING_KEY_CLASS_CONFIG)) {
                Class<?> routingKeyGeneratorClass = Class.forName(pravegaProps.get(WorkerConfig.ROUTING_KEY_CLASS_CONFIG));
                generator = (RoutingKeyGenerator) routingKeyGeneratorClass.newInstance();
            }
            for(SourceRecord record: records) {
                Object message = record.getValue();
                if (pravegaProps.containsKey(WorkerConfig.ROUTING_KEY_CLASS_CONFIG)) {
                    String routingKey = generator.generateRoutingKey(message);
                    if(routingKey == null)  {
                        final CompletableFuture writeFuture = writer.writeEvent(message);
                    }
                    else{
                        final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
                    }
                } else {
                    final CompletableFuture writeFuture = writer.writeEvent(message);
                }
                writer.flush();
            }

        } catch (Exception e) {
            logger.error("write msg error", e);
        }
    }

    public void close() {
        writer.close();
        clientFactory.close();
    }

}
