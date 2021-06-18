package io.pravega.connecter.runtime.sink;

import io.pravega.client.stream.EventRead;

import java.util.List;
import java.util.Map;

public interface Sink {
    void open(Map<String, String> sinkProps, Map<String, String> pravegaProps);

    void close();

    void write(List<SinkRecord> recordList);
}
