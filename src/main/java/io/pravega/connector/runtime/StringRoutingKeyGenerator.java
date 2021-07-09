package io.pravega.connector.runtime;

public class StringRoutingKeyGenerator implements RoutingKeyGenerator{
    @Override
    public String generateRoutingKey(Object message) {
        String msg = (String) message;
        if(msg.length() == 0) return "1";
        return msg.substring(0, 1);
    }
}
