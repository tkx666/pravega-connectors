package io.pravega.connector.runtime.configs;

import io.pravega.connector.runtime.Config;

import java.util.Map;

/**
 * SourceConfig defines the expected configuration of the Source task
 */
public class SourceConfig extends ConnectorConfig {
    public static String TRANSACTION_ENABLE_CONFIG = "transaction.enable";

    static Config.Validator validator = new Config.NonEmptyStringValidator();

    public static final Config basicConfig = ConnectorConfig.config()
            .add(TRANSACTION_ENABLE_CONFIG, Config.Type.STRING, "false", validator);

    public SourceConfig(Map<String, String> props) {
        super(basicConfig,  props);
    }
}
