package io.pravega.connector.runtime.sink;

import io.pravega.connector.runtime.Config;

import java.util.List;
import java.util.Map;

/**
 * Sink interface get records from the Pravega and write the SinkRecord to external system.
 */
public interface Sink {
    /**
     * define the configuration for parse and validation
     * @return the defined config
     */
    Config config();

    /**
     * initialize the sink
     * @param sinkProps connector properties from the command line
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
