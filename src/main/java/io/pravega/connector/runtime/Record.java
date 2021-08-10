package io.pravega.connector.runtime;

/**
 * Record contains the value that transfer among different systems
 */
public class Record {
    protected Object value;

    public Record(Object value) {
        this.value = value;
    }
}
