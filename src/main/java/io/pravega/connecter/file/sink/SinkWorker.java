package io.pravega.connecter.file.sink;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SinkWorker {
    private final ExecutorService executor;
    private Map<String, String> fileProps;
    private Map<String, String> pravegaProps;
    public SinkWorker(Map<String, String> fileProps, Map<String, String> pravegaProps){
        this.executor = new ThreadPoolExecutor(10, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.fileProps = fileProps;
    }
    public void execute(int nThread){
        FileSink fileSink = new FileSink();
        fileSink.open(fileProps, pravegaProps);
        PravegaReader.init(pravegaProps);

        for(int i = 0; i < nThread; i++)
        {
            try{
                PravegaReader pravegaReader = new PravegaReader(pravegaProps, "thread-" + i);
                SinkTask sinkTask = new SinkTask(pravegaReader, fileSink, pravegaProps);
                executor.submit(sinkTask);
            } catch (Exception e){
                e.printStackTrace();
            }

        }
        executor.shutdown();

    }
}
