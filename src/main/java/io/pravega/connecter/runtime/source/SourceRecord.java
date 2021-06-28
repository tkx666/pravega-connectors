package io.pravega.connecter.runtime.source;

import io.pravega.connecter.runtime.Record;

public class SourceRecord extends Record {
    public SourceRecord(Object value) {
        super(value);
    }

    public Object getValue() {
        return this.value;
    }
}
