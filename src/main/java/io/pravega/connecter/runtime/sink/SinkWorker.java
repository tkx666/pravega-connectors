package io.pravega.connecter.runtime.sink;

import io.pravega.connecter.file.sink.FileSink;
import io.pravega.connecter.runtime.PravegaReader;
import io.pravega.connecter.runtime.Worker;
import io.pravega.connecter.runtime.WorkerState;
import io.pravega.connecter.runtime.storage.MemoryTasksInfoStore;
import io.pravega.connecter.runtime.storage.TasksInfoStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SinkWorker implements Worker {
    private final ExecutorService executor;
    private Map<String, String> pravegaProps;
    private Map<String, String> sinkProps;
    private TasksInfoStore tasksInfoStore;
    public static String SINK_CLASS_CONFIG = "class";
    public static String SINK_NAME_CONFIG = "name";

    //    private Sink sink;
    public SinkWorker(Map<String, String> pravegaProps, Map<String, String> sinkProps) {
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.sinkProps = sinkProps;
        this.tasksInfoStore = new MemoryTasksInfoStore();
    }

    public void execute(int nThread) {
        Class<?> sinkClass = null;
        try {
            sinkClass = Class.forName(sinkProps.get(SINK_CLASS_CONFIG));
            PravegaReader.init(pravegaProps);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<PravegaReader> readerGroup = new ArrayList<>();
        for (int i = 0; i < nThread; i++) {
            try {
                readerGroup.add(new PravegaReader(pravegaProps, sinkProps.get(SINK_NAME_CONFIG) + i));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }


        for (int i = 0; i < nThread; i++) {
            try {
                //PravegaReader pravegaReader = new PravegaReader(pravegaProps, sinkProps.get("name") + i);
                Sink sink = (Sink) sinkClass.newInstance();
                sink.open(sinkProps, pravegaProps);
                SinkTask sinkTask = new SinkTask(readerGroup.get(i), sink, pravegaProps);
                executor.submit(sinkTask);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        executor.shutdown();

    }

    @Override
    public void setWorkerState(WorkerState workerState, String workerName) {

    }
}
