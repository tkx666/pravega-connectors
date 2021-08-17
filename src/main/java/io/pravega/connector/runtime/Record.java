package io.pravega.connector.runtime;

/**
 * Record encapsulates the value that transfer among different systems
 *
 * The value is a Object instance.
 */
public class Record {
    protected Object value;

    public Record(Object value) {
        this.value = value;
    }
}
