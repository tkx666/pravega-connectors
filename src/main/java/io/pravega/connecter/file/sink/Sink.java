package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;

import java.util.List;
import java.util.Map;

public interface Sink {
    void open(Map<String, String> fileProps, Map<String, String> pravegaProps);

    void close();

    void write(List<EventRead<String>> readList);
}
