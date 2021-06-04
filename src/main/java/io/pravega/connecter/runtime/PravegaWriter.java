package io.pravega.connecter.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PravegaWriter {
    public static String scope;
    public static String streamName;
    public static URI controllerURI;
    public static EventStreamClientFactory clientFactory;
    private EventStreamWriter<String> writer;

    public PravegaWriter(Map<String, String> pravegaProps) {
        this.scope = pravegaProps.get("scope");
        this.streamName = pravegaProps.get("name");
        this.controllerURI = URI.create(pravegaProps.get("uri"));
        this.writer = clientFactory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
    }

    public static void init(Map<String, String> pravegaProps) {
        scope = pravegaProps.get("scope");
        streamName = pravegaProps.get("name");
        controllerURI = URI.create(pravegaProps.get("uri"));
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(5))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());


    }

    public void run(String routingKey, String message) {
        System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message, routingKey, scope, streamName);
        final CompletableFuture writeFuture = writer.writeEvent(message);
    }

    public void close(){
        clientFactory.close();
        writer.close();
    }

}
