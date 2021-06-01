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
        synchronized (FileSink.class){
            try {
                while (true) {
//                    if(!queue.isEmpty()){
//                        System.out.println("write");
//                        out.write(queue.poll().getEvent());
//                        out.newLine();
//                        out.flush();
//                    }
                    out.write(queue.take().getEvent());
                    out.newLine();
                    out.flush();

                }
            } catch (IOException | InterruptedException e) {
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

    public void write() {
        try {
            while (true) {
                if(!queue.isEmpty()){
                    System.out.println("write");
                    out.write(queue.poll().getEvent());
                    out.newLine();
                    out.flush();
                }
                else break;

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
