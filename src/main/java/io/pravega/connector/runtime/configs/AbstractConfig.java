package io.pravega.connector.runtime.configs;

import io.pravega.connector.runtime.Config;
import io.pravega.connector.runtime.exception.ConfigException;

import java.util.Map;

/**
 * AbstractConfig is a basic config class that contains both original configuration and parsed configuration
 */
public class AbstractConfig {
    Map<String, String> stringConfig;
    Map<String, Object> parsedConfig;

    public AbstractConfig(Config config, Map<String, String> stringConfig) {
        this.stringConfig = stringConfig;
        this.parsedConfig = config.parse(stringConfig);
    }

    public Map<String, String> getStringConfig() {
        return stringConfig;
    }

    public Map<String, Object> getParsedConfig() {
        return parsedConfig;
    }

    public Object get(String key) {
        if (!parsedConfig.containsKey(key)) {
            throw new ConfigException("unkonw key");
        }
        return parsedConfig.get(key);
    }

    public String getString(String key) {
        return (String) get(key);
    }

    public Integer getInt(String key) {
        return (Integer) get(key);
    }
}
