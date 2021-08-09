package io.pravega.connector.runtime.sink;

import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.connector.runtime.PravegaReader;
import io.pravega.connector.runtime.Task;
import io.pravega.connector.runtime.Worker;
import io.pravega.connector.runtime.WorkerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
    private Map<String, List<Task>> tasks;
    private Map<String, Map<String, String>> connectors;
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
    public static String STREAM_NAME_CONFIG = "streamName";
    public static String SEGMENTS_NUM_CONFIG = "segments";

    public SinkWorker(Map<String, String> pravegaProps, Map<String, String> sinkProps) {
        this.executor = new ThreadPoolExecutor(20, 200, 60L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
        this.pravegaProps = pravegaProps;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(10);
        this.tasks = new HashMap<>();
        this.connectors = new HashMap<>();
    }

    public void startConnector(Map<String, String> connectorProps) {
        Class<?> sinkClass = null;
        int threadNum = Integer.valueOf(connectorProps.get(TASK_NUM_CONFIG));
        try {
            sinkClass = Class.forName(connectorProps.get(SINK_CLASS_CONFIG));
//            PravegaReader.init(pravegaProps, connectorProps);
            initializeReaderGroup(pravegaProps, connectorProps);
            connectors.put(connectorProps.get(SINK_NAME_CONFIG), connectorProps);
            List<PravegaReader> readerGroup = new ArrayList<>();
            for (int i = 0; i < threadNum; i++) {
                PravegaReader reader = new PravegaReader(pravegaProps, connectorProps.get(SINK_NAME_CONFIG) + i);
                reader.initialize(pravegaProps);
                readerGroup.add(reader);
            }
            for (int i = 0; i < threadNum; i++) {
                startSinkTask(sinkClass, connectorProps, readerGroup.get(i));
            }
            startCheckPoint(connectorProps);

        } catch (Exception e) {
            e.printStackTrace();
        }

//        executor.shutdown();

    }

    public void startSinkTask(Class taskClass, Map<String, String> connectorProps, PravegaReader pravegaReader) throws IllegalAccessException, InstantiationException {
        Sink sink = (Sink) taskClass.newInstance();
        sink.open(connectorProps, pravegaProps);
        SinkTask sinkTask = new SinkTask(pravegaReader, sink, pravegaProps, WorkerState.Started);
        tasks.putIfAbsent(connectorProps.get("name"), new ArrayList<>());
        tasks.get(connectorProps.get("name")).add(sinkTask);
        executor.submit(sinkTask);
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

    private boolean initializeReaderGroup(Map<String, String> pravegaProps, Map<String, String> connectorProps) throws IOException, ClassNotFoundException {
        String scope = pravegaProps.get(SCOPE_CONFIG);
        String streamName = pravegaProps.get(STREAM_NAME_CONFIG);
        URI controllerURI = URI.create(pravegaProps.get(URI_CONFIG));
        String readerGroup = pravegaProps.get(READER_GROUP_NAME_CONFIG);
        StreamManager streamManager = StreamManager.create(controllerURI);
        final boolean scopeIsNew = streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(Integer.valueOf(pravegaProps.get(SEGMENTS_NUM_CONFIG))))
                .build();
        final boolean streamIsNew = streamManager.createStream(scope, streamName, streamConfig);
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }
        if (hasCheckPoint(connectorProps)) {
            FileChannel fileChannel = new FileInputStream(connectorProps.get(CHECK_POINT_PATH_CONFIG)).getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            fileChannel.read(buffer);
            buffer.flip();
            Checkpoint checkpoint = Checkpoint.fromBytes(buffer);
            System.out.println("check point recover: " + checkpoint.getName());
            ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerURI);
            ReaderGroup existingGroup = readerGroupManager.getReaderGroup(readerGroup);

            System.out.println(existingGroup.getOnlineReaders());
            existingGroup.resetReaderGroup(ReaderGroupConfig
                    .builder()
                    .startFromCheckpoint(checkpoint)
                    .build());
        }
        streamManager.close();
        return true;

    }

    private boolean hasCheckPoint(Map<String, String> connectorProps) {
        File file = new File(connectorProps.get(CHECK_POINT_PATH_CONFIG));
        return file.exists();
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
    public void deleteTasksConfig(String connectorName) {
        if (tasks.containsKey(connectorName)) tasks.remove(connectorName);
        return;
    }

    @Override
    public void deleteConnectorConfig(String connectorName) {
        if (connectors.containsKey(connectorName)) connectors.remove(connectorName);
        return;
    }

    @Override
    public Map<String, String> getConnectorConfig(String connectorName) {
        return connectors.getOrDefault(connectorName, null);
    }
}
