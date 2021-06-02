package io.pravega.connecter.runtime.sink;

import io.pravega.connecter.file.sink.FileSink;
import io.pravega.connecter.runtime.PravegaReader;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SinkWorker {
    private final ExecutorService executor;
    private Map<String, String> fileProps;
    private Map<String, String> pravegaProps;
    private Map<String, String> sinkProps;
    private Sink sink;
    public SinkWorker(Map<String, String> fileProps, Map<String, String> pravegaProps, Map<String, String> sinkProps){
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.fileProps = fileProps;
        this.sinkProps = sinkProps;
    }
    public void execute(int nThread) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> sinkClass = Class.forName(sinkProps.get("class"));
        sink = (Sink) sinkClass.newInstance();
        // FileSink fileSink = new FileSink();
        sink.open(fileProps, pravegaProps);
        PravegaReader.init(pravegaProps);

        for(int i = 0; i < nThread; i++)
        {
            try{
                PravegaReader pravegaReader = new PravegaReader(pravegaProps, "thread-" + i);
                SinkTask sinkTask = new SinkTask(pravegaReader, sink, pravegaProps);
                executor.submit(sinkTask);
            } catch (Exception e){
                e.printStackTrace();
            }

        }
        executor.shutdown();

    }
}
