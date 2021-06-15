package io.pravega.connecter.runtime.source;

import java.util.List;
import java.util.Map;

public interface Source {
    void open(Map<String, String> fileProps, Map<String, String> pravegaProps);

    void close();

    List<SourceRecord> read();

}
