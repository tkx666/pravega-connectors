package io.pravega.connecter.runtime.source;

import io.pravega.connecter.runtime.PravegaReader;
import io.pravega.connecter.runtime.PravegaWriter;
import io.pravega.connecter.runtime.Worker;
import io.pravega.connecter.runtime.sink.Sink;
import io.pravega.connecter.runtime.sink.SinkTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SourceWorker implements Worker {
    private final ExecutorService executor;
    private Map<String, String> pravegaProps;
    private Map<String, String> sourceProps;
    public SourceWorker(Map<String, String> pravegaProps, Map<String, String> sourceProps){
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.sourceProps = sourceProps;
    }
    public void execute(int nThread){
        Class<?> sourceClass = null;
        List<Source> sourceGroup = new ArrayList<>();
        try {
            PravegaWriter.init(pravegaProps);
            for(int i = 0; i < nThread; i++){
                sourceClass = Class.forName(sourceProps.get("class"));
                Source source = (Source) sourceClass.newInstance();
                source.open(sourceProps, pravegaProps);
                sourceGroup.add(source);
            }
        } catch (Exception e){
            System.out.println(e.getMessage());
        }


        for(int i = 0; i < nThread; i++)
        {
            try{
                PravegaWriter pravegaWriter = new PravegaWriter(pravegaProps);
                SourceTask sourceTask = new SourceTask(pravegaWriter, sourceGroup.get(i), pravegaProps);
                executor.submit(sourceTask);
            } catch (Exception e){
                e.printStackTrace();
            }

        }
        executor.shutdown();

    }
}
