package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;
import io.pravega.connecter.runtime.sink.Sink;
import io.pravega.connecter.runtime.sink.SinkRecord;

import java.io.*;
import java.util.List;
import java.util.Map;

public class FileSink implements Sink {
    public static String WRITE_PATH_CONFIG = "writePath";
    Map<String, String> sinkProps;
    Map<String, String> pravegaProps;
    BufferedWriter out;
    @Override
    public void open(Map<String, String> sinkProps, Map<String, String> pravegaProps) {
        this.sinkProps = sinkProps;
        this.pravegaProps = pravegaProps;
        try {
            this.out = new BufferedWriter(new FileWriter(sinkProps.get(WRITE_PATH_CONFIG), true));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
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
            e.printStackTrace();
        } finally {
            try {
                out.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
