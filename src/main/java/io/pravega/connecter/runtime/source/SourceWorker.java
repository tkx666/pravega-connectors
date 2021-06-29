package io.pravega.connecter.runtime.source;

import io.pravega.connecter.runtime.*;
import io.pravega.connecter.runtime.sink.Sink;
import io.pravega.connecter.runtime.sink.SinkTask;
import io.pravega.connecter.runtime.storage.MemoryTasksInfoStore;
import io.pravega.connecter.runtime.storage.TasksInfoStore;

import java.util.ArrayList;
import java.util.HashMap;
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
    private Map<String, List<Task>> tasks;
    private volatile WorkerState workerState;
    public static String SOURCE_CLASS_CONFIG = "class";

    public SourceWorker(Map<String, String> pravegaProps, Map<String, String> sourceProps, WorkerState workerState) {
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.sourceProps = sourceProps;
        this.workerState = workerState;
        this.tasks = new HashMap<>();
    }

    public void execute(int nThread) {
        Class<?> sourceClass = null;
        List<Source> sourceGroup = new ArrayList<>();
        String workerName = sourceProps.get("name");
        try {
            PravegaWriter.init(pravegaProps);
            for (int i = 0; i < nThread; i++) {
                sourceClass = Class.forName(sourceProps.get(SOURCE_CLASS_CONFIG));
                Source source = (Source) sourceClass.newInstance();
                source.open(sourceProps, pravegaProps);
                sourceGroup.add(source);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (int i = 0; i < nThread; i++) {
            try {
                PravegaWriter pravegaWriter = new PravegaWriter(pravegaProps);
                SourceTask sourceTask = new SourceTask(pravegaWriter, sourceGroup.get(i), pravegaProps, WorkerState.Started);
                tasks.putIfAbsent(sourceProps.get("name"), new ArrayList<>());
                tasks.get(sourceProps.get("name")).add(sourceTask);
                executor.submit(sourceTask);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        executor.shutdown();

    }

    public void setWorkerState(WorkerState workerState, String workerName) {
        this.workerState = workerState;
        List<Task> tasksList = tasks.get(workerName);
        if(tasksList == null) return;
        for(Task task: tasksList) {
            task.setState(workerState);
        }
    }

}
