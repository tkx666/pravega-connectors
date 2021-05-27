package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;

import java.io.*;
import java.util.List;
import java.util.Map;

public class FileSink implements Sink{
    Map<String, String> fileProps;
    Map<String, String> pravegaProps;
    BufferedWriter out;
    PravegaReader pravegaReader;
    @Override
    public void open(Map<String, String> fileProps, Map<String, String> pravegaProps, PravegaReader pravegaReader) {
        this.fileProps = fileProps;
        this.pravegaProps = pravegaProps;
        this.pravegaReader = pravegaReader;
        try {
            this.out = new BufferedWriter(new FileWriter(fileProps.get("writePath")));
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
    public void write(List<EventRead<String>> readList) {
        try {
            for (EventRead<String> event : readList) {
                out.write(event.getEvent());
                out.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
