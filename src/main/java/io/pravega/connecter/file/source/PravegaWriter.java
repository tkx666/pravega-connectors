package io.pravega.connecter.file.source;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class PravegaWriter {
    public final String scope;
    public final String streamName;
    public final URI controllerURI;

    public PravegaWriter(Map<String, String> pravegaProps){
        this.scope = pravegaProps.get("scope");
        this.streamName = pravegaProps.get("name");
        this.controllerURI = URI.create(pravegaProps.get("uri"));
    }

    public void run(String routingKey, String message) {
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
             EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                     new JavaSerializer<String>(),
                     EventWriterConfig.builder().build())) {

            System.out.format("Writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                    message, routingKey, scope, streamName);
            final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
        }
    }

}
