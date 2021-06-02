package io.pravega.connecter.file.sink;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PravegaReader {
    public static String scope;
    public static String streamName;
    public static URI controllerURI;
    private static final String readerGroup = "group";
    private static final int READER_TIMEOUT_MS = 5000;
    private static EventStreamClientFactory clientFactory;
    private String readerName;


    public PravegaReader(Map<String, String> pravegaProps, String readerName) {
        this.scope = pravegaProps.get("scope");
        this.streamName = pravegaProps.get("name");
        this.controllerURI = URI.create(pravegaProps.get("uri"));
        this.readerName = readerName;
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

        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
    }

    public List<EventRead<String>> readEvent() {
            try (EventStreamReader<String> reader = clientFactory.createReader(readerName,
                    readerGroup,
                    new UTF8StringSerializer(),
                    ReaderConfig.builder().build())) {
                List<EventRead<String>> readList = new ArrayList<>();
                EventRead<String> event = null;
                do {
                    try {
                        event = reader.readNextEvent(READER_TIMEOUT_MS);
                        if (event.getEvent() != null) {
                            readList.add(event);
                            System.out.format("Read event '%s %s'%n", Thread.currentThread().getName(), event.getEvent());
                        }
                    } catch (ReinitializationRequiredException e) {
                        //There are certain circumstances where the reader needs to be reinitialized
                        e.printStackTrace();
                    }
                } while (event.getEvent() != null);
                return readList;
            }



//            String a = null;
//            while (true) {
//                System.out.println(Thread.currentThread().getName());
//                if((a = reader.readNextEvent(1000).getEvent()) != null)
//                    System.out.format("Read event '%s %s'%n", Thread.currentThread().getName(), a);
//                else break;
//            }
        //reader.close();


//        do {
//            try {
//                event = reader.readNextEvent(READER_TIMEOUT_MS);
//                if (event.getEvent() != null) {
//                    readList.add(event);
//                    System.out.format("Read event '%s %s'%n", Thread.currentThread().getName(), event.getEvent());
//                }
//            } catch (ReinitializationRequiredException e) {
//                //There are certain circumstances where the reader needs to be reinitialized
//                e.printStackTrace();
//            }
//        } while (event.getEvent() != null);
//        String a = null;
//        while (true) {
//            System.out.println(Thread.currentThread().getName());
//            if((a = reader.readNextEvent(1000).getEvent()) != null)
//                System.out.format("Read event '%s %s'%n", Thread.currentThread().getName(), a);
//            else break;
//        }

    }


}
