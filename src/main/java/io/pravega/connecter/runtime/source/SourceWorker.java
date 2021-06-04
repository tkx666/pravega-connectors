package io.pravega.connecter.runtime.source;

import io.pravega.connecter.runtime.PravegaReader;
import io.pravega.connecter.runtime.PravegaWriter;
import io.pravega.connecter.runtime.sink.Sink;
import io.pravega.connecter.runtime.sink.SinkTask;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SourceWorker {
    private final ExecutorService executor;
    private Map<String, String> fileProps;
    private Map<String, String> pravegaProps;
    private Map<String, String> sourceProps;
    private Source source;
    public SourceWorker(Map<String, String> fileProps, Map<String, String> pravegaProps, Map<String, String> sourceProps){
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.fileProps = fileProps;
        this.sourceProps = sourceProps;
    }
    public void execute(int nThread) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> sourceClass = Class.forName(sourceProps.get("class"));
        source = (Source) sourceClass.newInstance();
        source.open(fileProps, pravegaProps);
        PravegaWriter.init(pravegaProps);

        for(int i = 0; i < nThread; i++)
        {
            try{
                PravegaWriter pravegaWriter = new PravegaWriter(pravegaProps);
                SourceTask sourceTask = new SourceTask(pravegaWriter, source, pravegaProps);
                executor.submit(sourceTask);
            } catch (Exception e){
                e.printStackTrace();
            }

        }
        executor.shutdown();

    }
}
