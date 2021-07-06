package io.pravega.connecter.runtime.sink;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.connecter.file.sink.FileSink;
import io.pravega.connecter.runtime.PravegaReader;
import io.pravega.connecter.runtime.Task;
import io.pravega.connecter.runtime.Worker;
import io.pravega.connecter.runtime.WorkerState;
import io.pravega.connecter.runtime.storage.MemoryTasksInfoStore;
import io.pravega.connecter.runtime.storage.TasksInfoStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class SinkWorker implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(SinkWorker.class);
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutorService;
    private Map<String, String> pravegaProps;
    private Map<String, String> sinkProps;
    private Map<String, List<Task>> tasks;
    private volatile WorkerState workerState;


    public static String SINK_CLASS_CONFIG = "class";
    public static String SINK_NAME_CONFIG = "name";
    public static String CHECK_POINT_INTERVAL = "30";
    public static String SCOPE_CONFIG = "scope";
    public static String URI_CONFIG = "uri";
    public static String READER_GROUP_NAME_CONFIG = "readerGroup";
    public static String CHECK_POINT_NAME = "checkpoint.name";
    public static String TASK_NUM_CONFIG = "tasks.max";
    public static String CHECK_POINT_PATH_CONFIG = "checkpoint.persist.path";


    //    private Sink sink;
    public SinkWorker(Map<String, String> pravegaProps, Map<String, String> sinkProps) {
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.sinkProps = sinkProps;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(10);
        this.tasks = new HashMap<>();
    }

    public void execute() {
        Class<?> sinkClass = null;
        int threadNum = Integer.valueOf(sinkProps.get(TASK_NUM_CONFIG));
        try {
            sinkClass = Class.forName(sinkProps.get(SINK_CLASS_CONFIG));
            PravegaReader.init(pravegaProps, sinkProps);
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<PravegaReader> readerGroup = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            try {
                readerGroup.add(new PravegaReader(pravegaProps, sinkProps.get(SINK_NAME_CONFIG) + i));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }


        for (int i = 0; i < threadNum; i++) {
            try {
                Sink sink = (Sink) sinkClass.newInstance();
                sink.open(sinkProps, pravegaProps);
                SinkTask sinkTask = new SinkTask(readerGroup.get(i), sink, pravegaProps, WorkerState.Started);
                tasks.putIfAbsent(sinkProps.get("name"), new ArrayList<>());
                tasks.get(sinkProps.get("name")).add(sinkTask);
                executor.submit(sinkTask);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        startCheckPoint(sinkProps);
//        executor.shutdown();

    }

    @Override
    public void setWorkerState(WorkerState workerState, String workerName) {
        this.workerState = workerState;
        List<Task> tasksList = tasks.get(workerName);
        if (tasksList == null) return;
        for (Task task : tasksList) {
            task.setState(workerState);
        }
    }

    public void startCheckPoint(Map<String, String> sinkProps) {
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(pravegaProps.get(SCOPE_CONFIG), URI.create(pravegaProps.get(URI_CONFIG)));
        ReaderGroup group = readerGroupManager.getReaderGroup(pravegaProps.get(READER_GROUP_NAME_CONFIG));
        ScheduledFuture<?> future = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info("reader group {}", group.getOnlineReaders());
                CompletableFuture<Checkpoint> checkpointResult =
                        group.initiateCheckpoint(sinkProps.get(CHECK_POINT_NAME), Executors.newScheduledThreadPool(5));
                try {
                    Checkpoint checkpoint = checkpointResult.get(10, TimeUnit.SECONDS);
                    logger.info("check point start {}", checkpoint);
                    ByteBuffer serializedCheckPoint = checkpoint.toBytes();
                    FileChannel fileChannel = new FileOutputStream(sinkProps.get(CHECK_POINT_PATH_CONFIG)).getChannel();

                    fileChannel.write(serializedCheckPoint);
                    fileChannel.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }, 15, Long.parseLong(CHECK_POINT_INTERVAL), TimeUnit.SECONDS);
    }

    public void shutdownScheduledService() {
        scheduledExecutorService.shutdown();
    }

    @Override
    public void deleteTasksConfig(String worker) {
        if (tasks.containsKey(worker)) tasks.remove(worker);
        return;
    }
}
