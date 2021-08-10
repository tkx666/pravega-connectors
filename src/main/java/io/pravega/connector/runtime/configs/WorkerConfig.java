package io.pravega.connector.runtime.configs;

import io.pravega.connector.runtime.Config;

import java.util.Map;

public class WorkerConfig extends AbstractConfig {
    public static String SCOPE_CONFIG = "scope";
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String URI_CONFIG = "uri";
    public static String SERIALIZER_CONFIG = "serializer";
    public static String SEGMENTS_NUM_CONFIG = "segments";
    public static String READER_GROUP_CONFIG = "readerGroup";
    public static String REST_PORT_CONFIG = "rest.port";
    public static String ROUTING_KEY_CLASS_CONFIG = "routingKey.class";


    static Config.Validator validator = new Config.NonEmptyStringValidator();

    public static final Config config = new Config()
            .add(SCOPE_CONFIG, Config.Type.STRING, null, null)
            .add(STREAM_NAME_CONFIG, Config.Type.STRING, null, null)
            .add(URI_CONFIG, Config.Type.STRING, "tcp://127.0.0.1:9090", validator)
            .add(SERIALIZER_CONFIG, Config.Type.STRING, "io.pravega.client.stream.impl.UTF8StringSerializer", validator)
            .add(SEGMENTS_NUM_CONFIG, Config.Type.INT, "5", null)
            .add(READER_GROUP_CONFIG, Config.Type.STRING, null, null)
            .add(ROUTING_KEY_CLASS_CONFIG, Config.Type.STRING, "io.pravega.connector.runtime.DefaultRoutingKeyGenerator", validator)
            .add(REST_PORT_CONFIG, Config.Type.INT, "8091", null);

    public WorkerConfig(Map<String, String> props) {
        super(config, props);
    }

}
