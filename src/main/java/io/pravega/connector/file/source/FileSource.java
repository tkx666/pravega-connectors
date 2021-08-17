package io.pravega.connector.file.source;

import io.pravega.connector.runtime.Config;
import io.pravega.connector.runtime.source.Source;
import io.pravega.connector.runtime.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * FileSource implements the Source interface. This class initializes the BufferReader and read data from file
 */
public class FileSource implements Source {
    private static final Logger logger = LoggerFactory.getLogger(FileSource.class);
    public static String READ_PATH_CONFIG = "readPath";
    Map<String, String> sourceProps;
    BufferedReader in;
    @Override
    public Config config() {
        return null;
    }

    @Override
    public void open(Map<String, String> sourceProps) {
        this.sourceProps = sourceProps;
        try {
            this.in = new BufferedReader(new FileReader(sourceProps.get(READ_PATH_CONFIG)));
        } catch (FileNotFoundException e) {
            logger.error("", e);
        }
    }

    @Override
    public void close() {
        try {
            in.close();
        } catch (IOException e) {
            logger.error("", e);
        }
    }

    @Override
    public List<SourceRecord> read() {
        String str;
        List<SourceRecord> list = new ArrayList<>();
        try {
            if ((str = in.readLine()) != null) {
                list.add(new SourceRecord(str));
                return list;
            }
        } catch (IOException e) {
            logger.error("", e);
        }
        return list;
    }

}
