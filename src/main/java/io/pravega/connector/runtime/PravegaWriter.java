package io.pravega.connector.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.*;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PravegaWriter {
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
    public boolean initialize(Map<String, String> pravegaProps) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        Class serializerClass = Class.forName(pravegaProps.get(SERIALIZER_CONFIG));
        String scope = pravegaProps.get(SCOPE_CONFIG);
        URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
        String streamName = pravegaProps.get(STREAM_NAME_CONFIG);
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
        this.writer = clientFactory.createEventWriter(streamName,
                (Serializer)serializerClass.newInstance(),
                EventWriterConfig.builder().build());
        return true;
    }

    public void run(Object message) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
//        System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
//                message, routingKey, scope, streamName);
        Class routingKeyGeneratorClass = Class.forName(pravegaProps.get(ROUTING_KEY_CLASS_CONFIG));
        RoutingKeyGenerator generator = (RoutingKeyGenerator) routingKeyGeneratorClass.newInstance();
        String routingKey = generator.generateRoutingKey(message);
        final CompletableFuture writeFuture = writer.writeEvent(routingKey,message);
        writer.flush();
    }

    public void close(){
        writer.close();
    }

}
