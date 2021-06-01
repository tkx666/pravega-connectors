package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;
import io.pravega.connecter.file.source.FileSource;
import io.pravega.connecter.file.source.PravegaWriter;

import java.util.List;
import java.util.Map;

public class SinkTask implements Runnable{
    private Sink fileSink;
    private PravegaReader pravegaReader;
    private Map<String, String> pravegaProps;
    public SinkTask(PravegaReader pravegaReader, Sink fileSink, Map<String, String> pravegaProps){
        this.fileSink = fileSink;
        this.pravegaReader = pravegaReader;
        this.pravegaProps = pravegaProps;
    }
    @Override
    public void run() {
        List<EventRead<String>> readList = null;
        readList = pravegaReader.readEvent();
        System.out.println(Thread.currentThread() + "  size: " + readList.size());

        fileSink.write(readList);
    }
}
