package io.pravega.connecter.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.connecter.runtime.sink.SinkRecord;
import io.pravega.connecter.runtime.source.Source;
import io.pravega.connecter.runtime.source.SourceRecord;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PravegaReader {
    public static String scope;
    public static String streamName;
    public static Class<?> serializerClass;
    public static URI controllerURI;
    private static final String readerGroup = "group";
    private static final int READER_TIMEOUT_MS = 5000;
    private static EventStreamClientFactory clientFactory;
    private String readerName;
    private EventStreamReader<Object> reader;


    public PravegaReader(Map<String, String> pravegaProps, String readerName) throws IllegalAccessException, InstantiationException {
        this.readerName = readerName;
        this.reader = clientFactory.createReader(readerName,
                readerGroup,
                (Serializer) serializerClass.newInstance(),
                ReaderConfig.builder().build());

    }

    public static void init(Map<String, String> pravegaProps) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
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

        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
        streamManager.close();
    }

    public List<SinkRecord> readEvent() throws IllegalAccessException, InstantiationException {

        List<SinkRecord> readList = new ArrayList<>();
        EventRead<Object> event = null;
        do {
            try {
                event = reader.readNextEvent(READER_TIMEOUT_MS);
                if (event.getEvent() != null) {
                    readList.add(new SinkRecord(event.getEvent()));
//                    System.out.format("Read event '%s %s %s'%n", Thread.currentThread().getName(), event.getEvent(), event.getPosition());
                }
            } catch (ReinitializationRequiredException e) {
                //There are certain circumstances where the reader needs to be reinitialized
                e.printStackTrace();

            }
        } while (event.getEvent() != null);

        return readList;

    }
    public void close(){
        reader.close();
    }


}
