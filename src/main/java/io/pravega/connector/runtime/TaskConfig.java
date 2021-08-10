package io.pravega.connector.runtime;

import io.pravega.connector.runtime.configs.AbstractConfig;

import java.util.Map;

public class TaskConfig extends AbstractConfig {
    public TaskConfig(Config config, Map<String, String> stringConfig) {
        super(config, stringConfig);
    }
}
