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
    private static String readerGroup;
    private static final int READER_TIMEOUT_MS = 5000;
    private static EventStreamClientFactory clientFactory;
    private String readerName;
    private EventStreamReader<Object> reader;

    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String SEGMENTS_NUM_CONFIG = "segments";
    public static String READER_GROUP_NAME_CONFIG = "readerGroup";


    public PravegaReader(Map<String, String> pravegaProps, String readerName) throws IllegalAccessException, InstantiationException {
        this.readerName = readerName;
        this.reader = clientFactory.createReader(readerName,
                readerGroup,
                (Serializer) serializerClass.newInstance(),
                ReaderConfig.builder().build());

    }

    public static void init(Map<String, String> pravegaProps) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        scope = pravegaProps.get(SCOPE_CONFIG);
        streamName = pravegaProps.get(STREAM_NAME_CONFIG);
        controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
        serializerClass = Class.forName(pravegaProps.get(SERIALIZER_CONFIG));
        readerGroup = pravegaProps.get(READER_GROUP_NAME_CONFIG);
        StreamManager streamManager = StreamManager.create(controllerURI);

        final boolean scopeIsNew = streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(Integer.valueOf(pravegaProps.get(SEGMENTS_NUM_CONFIG))))
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
                e.printStackTrace();

            }
        } while (event.getEvent() != null);

        return readList;

    }

    public void close() {
        reader.close();
    }


}
