package io.pravega.connector.runtime;

public class Record {
    protected Object value;

    public Record(Object value) {
        this.value = value;
    }
}
