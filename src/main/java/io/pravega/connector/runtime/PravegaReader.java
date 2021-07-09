package io.pravega.connector.runtime;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.*;
import io.pravega.connector.runtime.sink.SinkRecord;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PravegaReader {
    private static final int READER_TIMEOUT_MS = 5000;
    private static EventStreamClientFactory clientFactory;
    private String readerName;
    private EventStreamReader<Object> reader;
    private Map<String, String> pravegaProps;
    private ReaderGroupConfig readerGroupConfig;

    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String SEGMENTS_NUM_CONFIG = "segments";
    public static String READER_GROUP_NAME_CONFIG = "readerGroup";
    public static String CHECK_POINT_PATH_CONFIG = "checkpoint.persist.path";


    public PravegaReader(Map<String, String> pravegaProps, String readerName) throws IllegalAccessException, InstantiationException {
        this.readerName = readerName;
//        this.reader = clientFactory.createReader(readerName,
//                readerGroup,
//                (Serializer) serializerClass.newInstance(),
//                ReaderConfig.builder().build());
        this.pravegaProps = pravegaProps;

    }

    public boolean initialize(Map<String, String> pravegaProps) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class serializerClass = Class.forName(pravegaProps.get(SERIALIZER_CONFIG));
        String readerGroup = pravegaProps.get(READER_GROUP_NAME_CONFIG);
        String scope = pravegaProps.get(SCOPE_CONFIG);
        URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));

        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
        this.reader = clientFactory.createReader(readerName,
                readerGroup,
                (Serializer) serializerClass.newInstance(),
                ReaderConfig.builder().build());
        return true;
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

    public static boolean hasCheckPoint(Map<String, String> connectorProps) {
        File file = new File(connectorProps.get(CHECK_POINT_PATH_CONFIG));
        return file.exists();
    }


}
