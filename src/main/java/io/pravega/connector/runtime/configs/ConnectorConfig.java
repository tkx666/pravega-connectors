package io.pravega.connector.runtime.configs;

import io.pravega.connector.runtime.Config;

import java.util.Map;

/**
 * ConnectorConfig defines the expected configuration of the connector
 */
public class ConnectorConfig extends AbstractConfig {
    public static String TYPE_CONFIG = "type";
    public static String TASKS_NUM_CONFIG = "tasks.max";
    public static String NAME_CONFIG = "name";
    public static String CLASS_CONFIG = "class";
    static Config.Validator validator = new Config.NonEmptyStringValidator();


    public static Config config() {
        return new Config()
                .add(TYPE_CONFIG, Config.Type.STRING, null, null)
                .add(TASKS_NUM_CONFIG, Config.Type.INT, "1", null)
                .add(NAME_CONFIG, Config.Type.STRING, null, null)
                .add(CLASS_CONFIG, Config.Type.STRING, null, null);
    }

    public ConnectorConfig(Config config, Map<String, String> stringConfig) {
        super(config, stringConfig);
    }
}
