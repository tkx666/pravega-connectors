package io.pravega.connector.runtime;

import io.pravega.connector.runtime.source.SourceRecord;

import java.util.List;

public interface Writer {
    void write(List<SourceRecord> records);
    void close();
    boolean initialize();

}
