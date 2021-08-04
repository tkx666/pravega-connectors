package io.pravega.connector.file.sink;

import io.pravega.connector.runtime.Config;
import io.pravega.connector.runtime.sink.Sink;
import io.pravega.connector.runtime.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FileSink implements Sink {
    private static final Logger logger = LoggerFactory.getLogger(FileSink.class);
    public static String WRITE_PATH_CONFIG = "writePath";
    Map<String, String> sinkProps;
    BufferedWriter out;

    @Override
    public Config config() {
        return null;
    }

    @Override
    public void open(Map<String, String> sinkProps) {
        this.sinkProps = sinkProps;
        try {
            this.out = new BufferedWriter(new FileWriter(sinkProps.get(WRITE_PATH_CONFIG), true));
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            logger.error("", e);
        }

    }

    @Override
    public void write(List<SinkRecord> recordList) {
        try {
            for (SinkRecord record : recordList) {
                out.write(record.getValue().toString());
                out.newLine();
            }

        } catch (IOException e) {
            logger.error("", e);
        } finally {
            try {
                out.flush();
            } catch (IOException e) {
                logger.error("", e);
            }
        }

    }

}
