package io.pravega.connecter.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PravegaWriter {
    public static String scope;
    public static String streamName;
    public static URI controllerURI;
    public static Class<?> serializerClass;

    public static EventStreamClientFactory clientFactory;
    private EventStreamWriter<Object> writer;
    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String SEGMENTS_NUM_CONFIG = "segments";

    public PravegaWriter(Map<String, String> pravegaProps) throws IllegalAccessException, InstantiationException {
        System.out.println(serializerClass);
        this.writer = clientFactory.createEventWriter(streamName,
                (Serializer)serializerClass.newInstance(),
                EventWriterConfig.builder().build());
    }

    public static void init(Map<String, String> pravegaProps) throws ClassNotFoundException {
        scope = pravegaProps.get(SCOPE_CONFIG);
        streamName = pravegaProps.get(STREAM_NAME_CONFIG);
        controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
        serializerClass = Class.forName(pravegaProps.get(SERIALIZER_CONFIG));
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(Integer.valueOf(pravegaProps.get(SEGMENTS_NUM_CONFIG))))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
        streamManager.close();


    }

    public void run(String routingKey, Object message) {
//        System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
//                message, routingKey, scope, streamName);
        final CompletableFuture writeFuture = writer.writeEvent(message);
        writer.flush();
    }

    public void close(){
        writer.close();
        clientFactory.close();
    }

}
