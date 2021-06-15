package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;
import io.pravega.connecter.runtime.sink.Sink;
import io.pravega.connecter.runtime.sink.SinkRecord;

import java.io.*;
import java.util.List;
import java.util.Map;

public class FileSink implements Sink {
    Map<String, String> sinkProps;
    Map<String, String> pravegaProps;
    BufferedWriter out;
    @Override
    public void open(Map<String, String> sinkProps, Map<String, String> pravegaProps) {
        this.sinkProps = sinkProps;
        this.pravegaProps = pravegaProps;
        try {
            this.out = new BufferedWriter(new FileWriter(sinkProps.get("writePath"), true));
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
    public void write(List<SinkRecord> readList) {
        try {
            for (SinkRecord record : readList) {
                out.write((String) record.getValue());
                out.newLine();
            }
//            synchronized (FileSink.class){
//                for (int i = 0; i < 10000; i++) {
//                    out.write(Integer.toString(i));
//                    out.newLine();
//                }
//            }

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
