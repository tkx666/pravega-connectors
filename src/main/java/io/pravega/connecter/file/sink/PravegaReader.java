package io.pravega.connecter.file.sink;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PravegaReader {
    public final String scope;
    public final String streamName;
    public final URI controllerURI;
    private static final int READER_TIMEOUT_MS = 2000;


    public PravegaReader(Map<String, String> pravegaProps){
        this.scope = pravegaProps.get("scope");
        this.streamName = pravegaProps.get("name");
        this.controllerURI = URI.create(pravegaProps.get("uri"));
    }

    public List<EventRead<String>> run() {
        StreamManager streamManager = StreamManager.create(controllerURI);

        final boolean scopeIsNew = streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);

        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
             EventStreamReader<String> reader = clientFactory.createReader("reader",
                     readerGroup,
                     new JavaSerializer<String>(),
                     ReaderConfig.builder().build())) {
            System.out.format("Reading all the events from %s/%s%n", scope, streamName);
            EventRead<String> event = null;
            List<EventRead<String>> readList = new ArrayList<>();
            do {
                try {
                    event = reader.readNextEvent(READER_TIMEOUT_MS);
                    if (event.getEvent() != null) {
                        readList.add(event);
                        System.out.format("Read event '%s'%n", event.getEvent());
                    }
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    e.printStackTrace();
                }
            } while (event.getEvent() != null);
            return readList;
        }
    }



}
