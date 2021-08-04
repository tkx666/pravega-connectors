package io.pravega.connector.runtime;

import java.util.Map;

public class SourceBasicConfig extends AbstractConfig{
    public static String TYPE_CONFIG = "type";
    public static String TASKS_NUM_CONFIG = "tasks.max";
    public static String NAME_CONFIG = "name";
    public static String CLASS_CONFIG = "class";
    public static String ROUTING_KEY_CLASS_CONFIG = "routingKey.class";
    public static String TRANSACTION_ENABLE_CONFIG = "transaction.enable";

    static Config.Validator validator = new Config.NonEmptyStringValidator();

    public static final Config basicConfig = new Config()
            .add(TYPE_CONFIG, Config.Type.STRING, "source", validator)
            .add(TASKS_NUM_CONFIG, Config.Type.STRING, "1", validator)
            .add(NAME_CONFIG, Config.Type.STRING, null, null)
            .add(CLASS_CONFIG, Config.Type.STRING, null, null)
            .add(ROUTING_KEY_CLASS_CONFIG, Config.Type.STRING, "io.pravega.connector.runtime.DefaultRoutingKeyGenerator", validator)
            .add(TRANSACTION_ENABLE_CONFIG, Config.Type.STRING, "false", validator);

    public SourceBasicConfig(Map<String, String> props) {
        super(basicConfig,  props);
    }
}
