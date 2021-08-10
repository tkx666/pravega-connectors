package io.pravega.connector.runtime.sink;

import io.pravega.connector.runtime.Config;

import java.util.List;
import java.util.Map;

public interface Sink {
    /**
     * define the configuration for parse and validation
     * @return the defined config
     */
    Config config();

    /**
     * initialize the sink
     * @param sinkProps sink properties
     */
    void open(Map<String, String> sinkProps);

    /**
     * write the SinkRecord read from Pravega to another system
     * @param recordList
     */
    void write(List<SinkRecord> recordList);

    /**
     * close the sink task
     */
    void close();

}
