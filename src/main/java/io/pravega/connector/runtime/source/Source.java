package io.pravega.connector.runtime.source;

import io.pravega.connector.runtime.Config;

import java.util.List;
import java.util.Map;

public interface Source {
    Config config();

    void open(Map<String, String> sourceProps);

    void close();

    List<SourceRecord> read();

}
