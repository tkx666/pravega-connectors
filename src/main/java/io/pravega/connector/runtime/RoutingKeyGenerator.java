package io.pravega.connector.runtime;

public interface RoutingKeyGenerator {
    String generateRoutingKey(Object message);
}
