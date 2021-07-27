package io.pravega.connector.runtime.exception;

public class ConfigException extends RuntimeException{
    public ConfigException(String msg) {
        super(msg);
    }
}
