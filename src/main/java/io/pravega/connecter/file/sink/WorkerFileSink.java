package io.pravega.connecter.file.sink;

import io.pravega.client.stream.EventRead;
import io.pravega.connecter.file.source.FileSource;
import io.pravega.connecter.file.source.PravegaWriter;

import java.util.List;
import java.util.Map;

public class WorkerFileSink implements Runnable{
    private FileSink fileSink;
    private PravegaReader pravegaReader;
    private Map<String, String> pravegaProps;
    public WorkerFileSink(PravegaReader pravegaReader, FileSink fileSink, Map<String, String> pravegaProps){
        this.fileSink = fileSink;
        this.pravegaReader = pravegaReader;
        this.pravegaProps = pravegaProps;
    }
    @Override
    public void run() {
        List<EventRead<String>> readList = pravegaReader.run();
        fileSink.write(readList);
    }
}
