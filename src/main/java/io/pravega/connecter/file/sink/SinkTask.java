package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;
import io.pravega.connecter.file.source.FileSource;
import io.pravega.connecter.file.source.PravegaWriter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public class SinkTask implements Runnable{
    private Sink fileSink;
    private PravegaReader pravegaReader;
    private Map<String, String> pravegaProps;
    private LinkedBlockingDeque<EventRead<String>> queue;
    public SinkTask(PravegaReader pravegaReader, Sink fileSink, Map<String, String> pravegaProps, LinkedBlockingDeque<EventRead<String>> queue){
        this.fileSink = fileSink;
        this.pravegaReader = pravegaReader;
        this.pravegaProps = pravegaProps;
        this.queue = queue;
    }
    @Override
    public void run() {
        List<EventRead<String>> readList = null;
        readList = pravegaReader.readEvent();
        System.out.println(Thread.currentThread().getName() + "  size:" + readList.size());

        fileSink.write(readList);
    }
}
