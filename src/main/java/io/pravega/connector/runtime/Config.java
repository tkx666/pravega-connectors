package io.pravega.connector.runtime;

import io.pravega.connector.runtime.exception.ConfigException;

import java.util.HashMap;
import java.util.Map;

/**
 * Config is used to define a set of expected configuration. You can custom the configuration and set the name, type,
 * default value and validator. The class can parse and validate the original configuration.
 */
public class Config {
    Map<String, ConfigInfo> configInfoMap;

    public Config() {
        this.configInfoMap = new HashMap<>();
    }

    public Config add(ConfigInfo configInfo) {
        if (configInfoMap.containsKey(configInfo.name)) {
            throw new ConfigException("duplicate config : " + configInfo.name);
        }
        configInfoMap.put(configInfo.name, configInfo);
        return this;
    }

    public Config add(String name, Type type, Object defaultValue, Validator validator) {
        return add(new ConfigInfo(name, type, defaultValue, validator));
    }

    public Map<String, Object> parse(Map<String, String> props) {
        Map<String, Object> parsedConfig = new HashMap<>();
        for(ConfigInfo configInfo: configInfoMap.values()) {
            parsedConfig.put(configInfo.name, parseValue(configInfo, props.get(configInfo.name), props.containsKey(configInfo.name)));
        }
        return parsedConfig;
    }
    public Object parseValue(ConfigInfo configInfo, Object value, boolean exist) {
        Object parsedValue;
        if(exist) {
            parsedValue = parseValue(configInfo.name, configInfo.type, value);
        }
        else {
            if (configInfo.defaultValue == null) {
                throw new ConfigException("the default value of " + configInfo.name + " is null. Provide the value in properties file");
            }
            parsedValue = configInfo.defaultValue;
        }
        if(configInfo.validator != null) {
            configInfo.validator.checkValid(parsedValue);
        }
        return parsedValue;
    }

    public Object parseValue(String name, Type type, Object propsValue) {
        String trimmedValue = null;
        if (propsValue instanceof String)
            trimmedValue = ((String) propsValue).trim();
        switch (type) {
            case STRING:
                if (propsValue instanceof String)
                    return trimmedValue;
                else if (propsValue instanceof Integer)
                    return String.valueOf(propsValue);
            case INT:
                if (propsValue instanceof String) {
                    return Integer.parseInt(trimmedValue);
                }
                if (propsValue instanceof Integer)
                    return propsValue;
            default:
                throw new IllegalStateException("unknown type");
        }

    }

    public static class ConfigInfo {
        public final String name;
        public final Type type;
        public final Object defaultValue;
        public final Validator validator;

        public ConfigInfo(String name, Type type, Object defaultValue, Validator validator) {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
            this.validator = validator;
        }
    }

    public enum Type {
        STRING, INT
    }

    public interface Validator {
        public boolean checkValid(Object value);
    }

    public static class NonEmptyStringValidator implements Validator {

        @Override
        public boolean checkValid(Object value) {
            String v = (String) value;
            if (v == null || (v != null && v.isEmpty()))
                return false;
            else
                return true;
        }
    }
}


