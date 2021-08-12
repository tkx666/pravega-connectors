package io.pravega.connector.runtime.source;

import io.pravega.connector.runtime.Config;

import java.util.List;
import java.util.Map;

public interface Source {
    /**
     * define the configuration for parse and validation
     *
     * @return the defined config
     */
    Config config();

    /**
     * initialize the sink
     *
     * @param sourceProps sink properties
     */
    void open(Map<String, String> sourceProps);

    /**
     * read the SourceRecord from other system
     *
     * @return a list of SourceRecord
     */
    List<SourceRecord> read();


    /**
     * close the sink task
     */
    void close();

}
