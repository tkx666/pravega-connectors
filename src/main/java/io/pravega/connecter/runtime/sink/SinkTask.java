package io.pravega.connecter.runtime.sink;

import io.pravega.client.stream.EventRead;
import io.pravega.connecter.runtime.PravegaReader;

import java.util.List;
import java.util.Map;

public class SinkTask implements Runnable{
    private Sink sink;
    private PravegaReader pravegaReader;
    private Map<String, String> pravegaProps;
    public SinkTask(PravegaReader pravegaReader, Sink sink, Map<String, String> pravegaProps){
        this.sink = sink;
        this.pravegaReader = pravegaReader;
        this.pravegaProps = pravegaProps;
    }
    @Override
    public void run() {
        List<SinkRecord> readList = null;
        while(true){
            try {
                readList = pravegaReader.readEvent();
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread() + "  size: " + readList.size());
            if(readList.size() == 0) break;
            sink.write(readList);

        }
        sink.close();
        pravegaReader.close();

    }
}
