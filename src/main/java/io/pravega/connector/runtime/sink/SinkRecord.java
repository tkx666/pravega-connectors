package io.pravega.connector.runtime.sink;

import io.pravega.connector.runtime.Record;

/**
 * SinkRecord is a Record for Sink connector
 */
public class SinkRecord extends Record {
    public SinkRecord(Object value) {
        super(value);
    }

    public Object getValue() {
        return this.value;
    }
}
