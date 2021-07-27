package io.pravega.connector.runtime;

import io.pravega.connector.runtime.exception.ConfigException;
import scala.concurrent.impl.FutureConvertersImpl;

import java.util.HashMap;
import java.util.Map;

public class Config {
    Map<String, ConfigInfo> configInfoMap;
    public Config() {
        configInfoMap = new HashMap<>();
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

    public Map<String, String> validate(Map<String, String> props) {
        for (String name : configInfoMap.keySet()) {
            parse(name, props);
        }
        return props;
    }

    public void parse(String name, Map<String, String> props) {
        if (!configInfoMap.containsKey(name)) return;
        ConfigInfo configInfo = configInfoMap.get(name);
        Object value;
        //support string only
        if(props.containsKey(name)) {
            value = parseValue(name, configInfo.type, props.get(name));
        }
        else {
            if(configInfo.defaultValue == null) {
                throw new ConfigException("the default value of " + name + " is null. Provide the value in properties file");
            }
            value = configInfo.defaultValue;
        }
        props.put(name, (String) value);
        Validator validator = configInfoMap.get(name).validator;
        if(validator != null) {
            if(!validator.checkValid(value)) {
                throw new ConfigException("key " + name + " has invalid value " + value);
            }
        }

    }

    public Object parseValue(String name, Type type, Object propsValue) {
        switch (type) {
            case STRING:
                if(propsValue instanceof String)
                    return propsValue;
                else if(propsValue instanceof Integer)
                    return String.valueOf(propsValue);
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
            if(v == null || (v != null && v.isEmpty()))
                return false;
            else
                return true;
        }
    }
}


