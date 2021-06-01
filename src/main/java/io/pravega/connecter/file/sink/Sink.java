package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public interface Sink {
    void open(Map<String, String> fileProps, Map<String, String> pravegaProps, LinkedBlockingDeque<EventRead<String>> queue);

    void close();

    void write(List<EventRead<String>> readList);
}
