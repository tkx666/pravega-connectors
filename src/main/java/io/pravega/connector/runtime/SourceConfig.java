package io.pravega.connector.runtime;

import java.util.Map;

public class SourceConfig extends ConnectorConfig{
    public static String TYPE_CONFIG = "type";
    public static String TASKS_NUM_CONFIG = "tasks.max";
    public static String NAME_CONFIG = "name";
    public static String CLASS_CONFIG = "class";
    public static String TRANSACTION_ENABLE_CONFIG = "transaction.enable";

    static Config.Validator validator = new Config.NonEmptyStringValidator();

    public static final Config basicConfig = ConnectorConfig.config()
            .add(TRANSACTION_ENABLE_CONFIG, Config.Type.STRING, "false", validator);

    public SourceConfig(Map<String, String> props) {
        super(basicConfig,  props);
    }
}
