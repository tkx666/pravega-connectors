package io.pravega.connector.runtime.sink;

import io.pravega.connector.runtime.Config;

import java.util.List;
import java.util.Map;

public interface Sink {
    Config config();

    void open(Map<String, String> sinkProps);

    void close();

    void write(List<SinkRecord> recordList);
}
