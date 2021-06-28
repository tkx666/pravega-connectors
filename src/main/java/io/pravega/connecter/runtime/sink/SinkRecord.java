package io.pravega.connecter.runtime.sink;

import io.pravega.connecter.runtime.Record;

public class SinkRecord extends Record {
    public SinkRecord(Object value) {
        super(value);
    }

    public Object getValue() {
        return this.value;
    }
}
