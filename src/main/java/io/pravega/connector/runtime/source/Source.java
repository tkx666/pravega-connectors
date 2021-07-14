package io.pravega.connector.runtime.source;

import java.util.List;
import java.util.Map;

public interface Source {
    void open(Map<String, String> sourceProps, Map<String, String> pravegaProps);

    void close();

    List<SourceRecord> read();

}
