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

    public PravegaWriter(Map<String, String> pravegaProps) throws IllegalAccessException, InstantiationException {
        System.out.println(serializerClass);
        this.writer = clientFactory.createEventWriter(streamName,
                (Serializer)serializerClass.newInstance(),
                EventWriterConfig.builder().build());
    }

    public static void init(Map<String, String> pravegaProps) throws ClassNotFoundException {
        scope = pravegaProps.get("scope");
        streamName = pravegaProps.get("name");
        controllerURI = URI.create(pravegaProps.get("uri"));
        serializerClass = Class.forName(pravegaProps.get("serializer"));
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(Integer.valueOf(pravegaProps.get("segments"))))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
        streamManager.close();


    }

    public void run(String routingKey, Object message) {
        System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message, routingKey, scope, streamName);
        final CompletableFuture writeFuture = writer.writeEvent(message).exceptionally( ex -> {
            System.out.println("wirte to pravega exception: " + ex.getMessage());
            return null;
        });
    }

    public void close(){
        writer.close();
        clientFactory.close();
    }

}
