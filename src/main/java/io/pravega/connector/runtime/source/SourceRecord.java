package io.pravega.connector.runtime.source;

import io.pravega.connector.runtime.Record;

/**
 * SourceRecord is a Record for Source connector
 */
public class SourceRecord extends Record {
    public SourceRecord(Object value) {
        super(value);
    }

    public Object getValue() {
        return this.value;
    }
}
