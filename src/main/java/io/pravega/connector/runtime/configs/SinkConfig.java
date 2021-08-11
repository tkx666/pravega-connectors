package io.pravega.connector.runtime.configs;

import io.pravega.connector.runtime.Config;

import java.util.Map;

/**
 * SinkConfig defines the expected configuration of the Sink task
 */
public class SinkConfig extends ConnectorConfig {
    public static String CHECKPOINT_PERSIST_PATH_CONFIG = "checkpoint.persist.path";
    public static String CHECKPOINT_NAME_CONFIG = "checkpoint.name";
    public static String CHECKPOINT_ENABLE_CONFIG = "checkpoint.enable";

    static Config.Validator validator = new Config.NonEmptyStringValidator();

    public static final Config basicConfig = ConnectorConfig.config()
            .add(CHECKPOINT_PERSIST_PATH_CONFIG, Config.Type.STRING, null, null)
            .add(CHECKPOINT_NAME_CONFIG, Config.Type.STRING, null, null)
            .add(CHECKPOINT_ENABLE_CONFIG, Config.Type.STRING, "true", validator);
    public SinkConfig(Map<String, String> props) {
        super(basicConfig,  props);
    }

}
