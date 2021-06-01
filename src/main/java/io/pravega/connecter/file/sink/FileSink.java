package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public class FileSink implements Sink{
    private Map<String, String> fileProps;
    private Map<String, String> pravegaProps;
    private BufferedWriter out;
    private LinkedBlockingDeque<EventRead<String>> queue;
    @Override
    public void open(Map<String, String> fileProps, Map<String, String> pravegaProps, LinkedBlockingDeque<EventRead<String>> queue) {
        this.fileProps = fileProps;
        this.pravegaProps = pravegaProps;
        this.queue = queue;
        try {
            this.out = new BufferedWriter(new FileWriter(fileProps.get("writePath"), true));
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
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
